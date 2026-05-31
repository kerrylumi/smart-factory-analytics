"""
Elering NPS päevahindade ingestion: API → bronze.br_electricity_prices.

Käivitub kord päevas (@daily, UTC kesköö). Laadib eelmise päeva 15-min
elektrihinnad iga riigi kohta (ee/fi/lv/lt). ON CONFLICT DO NOTHING tagab
idempotentsuse. Esimene käivitus loob ka schema ja tabeli.

Voog:
    ensure_schema >> laadi_hinnad >> dbt_run >> dbt_test
"""

import os
import ssl
import uuid
from datetime import datetime, timezone, timedelta

import psycopg2
import requests
import urllib3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from requests.adapters import HTTPAdapter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class _LaxHTTPSAdapter(HTTPAdapter):
    """Python 3.12+ + Docker MTU TLS workaround (sama muster kursuse näidisprojektis).

    Ei sobi produktsiooni — testkeskkonnas avaliku Elering API jaoks aktsepteeritav,
    kuna andmed pole tundlikud.
    """

    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0)
        kwargs["ssl_context"] = ctx
        super().init_poolmanager(*args, **kwargs)


def _session() -> requests.Session:
    s = requests.Session()
    s.mount("https://", _LaxHTTPSAdapter())
    return s


API_URL = "https://dashboard.elering.ee/api/nps/price"
COUNTRIES = ["ee", "fi", "lv", "lt"]


def _db_conn():
    """Loob psycopg2 ühenduse pgDuckDB analüütika andmebaasiga (teenus `db`)."""
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "db"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "praktikum"),
        password=os.environ.get("POSTGRES_PASSWORD", "praktikum"),
        dbname=os.environ.get("POSTGRES_DB", "praktikum"),
    )


def _execute(sql: str, params=None):
    with _db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)


def ensure_schema(**context):
    """Loob bronze skeemi ja tabelid, kui need puuduvad.

    Sprint 3-s asendub `init/01_create_schemas.sql` volume'iga compose.yml-s,
    kuid praegu tagab DAG ise iseseisva töötamise.
    """
    _execute(
        """
        CREATE SCHEMA IF NOT EXISTS bronze;
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE SCHEMA IF NOT EXISTS gold;

        CREATE TABLE IF NOT EXISTS bronze.pipeline_runs (
            run_id        uuid        PRIMARY KEY,
            fetched_at    timestamptz NOT NULL,
            source_name   text        NOT NULL,
            status        text        NOT NULL,
            message       text
        );

        CREATE TABLE IF NOT EXISTS bronze.br_electricity_prices (
            run_id         uuid          NOT NULL REFERENCES bronze.pipeline_runs(run_id),
            country        text          NOT NULL,
            ts_utc         timestamptz   NOT NULL,
            price_eur_mwh  numeric(10,4) NOT NULL,
            loaded_at      timestamptz   NOT NULL DEFAULT now(),
            PRIMARY KEY (country, ts_utc)
        );
        """
    )


def laadi_hinnad(logical_date, **context):
    """Laeb 24h aknaga eelmise päeva hinnad kõigi nelja riigi kohta.

    @daily DAG'i käivitumisel UTC keskööl on `logical_date` eelmise päeva algus,
    seega päring [logical_date, logical_date + 24h).
    """
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    start = logical_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1) - timedelta(milliseconds=1)

    _execute(
        """
        INSERT INTO bronze.pipeline_runs (run_id, fetched_at, source_name, status)
        VALUES (%s, %s, 'elering-nps', 'running')
        """,
        (run_id, now),
    )

    try:
        resp = _session().get(
            API_URL,
            params={
                "start": start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "end":   end.strftime("%Y-%m-%dT%H:%M:%S.999Z"),
            },
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        if not payload.get("success"):
            raise RuntimeError(f"Elering API success=False: {payload}")

        data = payload["data"]
        total = 0
        for country in COUNTRIES:
            rows = data.get(country, [])
            tuples = [
                (
                    run_id,
                    country,
                    datetime.fromtimestamp(item["timestamp"], tz=timezone.utc),
                    item["price"],
                )
                for item in rows
            ]
            if not tuples:
                continue
            placeholders = ",".join(["(%s, %s, %s, %s)"] * len(tuples))
            flat = [v for tup in tuples for v in tup]
            _execute(
                f"""
                INSERT INTO bronze.br_electricity_prices
                    (run_id, country, ts_utc, price_eur_mwh)
                VALUES {placeholders}
                ON CONFLICT (country, ts_utc) DO NOTHING
                """,
                flat,
            )
            total += len(tuples)

        _execute(
            "UPDATE bronze.pipeline_runs SET status = 'success', message = %s WHERE run_id = %s",
            (f"loaded {total} rows for window {start.isoformat()} .. {end.isoformat()}", run_id),
        )

    except Exception as exc:
        _execute(
            "UPDATE bronze.pipeline_runs SET status = 'failed', message = %s WHERE run_id = %s",
            (str(exc), run_id),
        )
        raise


with DAG(
    dag_id="elering_ingest",
    description="Laeb Elering NPS päevahinnad (15-min) bronze kihti ja käivitab dbt",
    schedule="@daily",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["elering", "bronze"],
) as dag:

    ensure = PythonOperator(
        task_id="ensure_schema",
        python_callable=ensure_schema,
    )

    lae = PythonOperator(
        task_id="laadi_hinnad",
        python_callable=laadi_hinnad,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt run --profiles-dir . --select silver"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt test --profiles-dir . --select silver"
        ),
    )

    ensure >> lae >> dbt_run >> dbt_test
