"""
Gold kihi perioodiline uuendus: dbt run --select +gold + testid.

Käivitub iga 5 minuti tagant. Sõltumatu Elering DAG-ist —
gold OEE kihis kasutatakse tehase telemeetria andmeid (bronze.raw_factory_data),
mitte Elering elektrihinna andmeid.

Selektor `+gold` ehitab ka upstream silver view'd uuesti (CREATE OR REPLACE VIEW,
praktiliselt tasuta), et nende definitsioonid oleksid alati ajakohased.
Andmeväärskuse tagab juba see, et silver on view bronze'i peal.

Voog:
    dbt_run_gold >> dbt_test_gold
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dbt_gold_refresh",
    description="Uuendab gold kihi OEE ja energia KPI-d iga 5 min",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 6, 6),
    catchup=False,
    tags=["dbt", "gold"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run_gold",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt run --profiles-dir . --select +gold"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test_gold",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt test --profiles-dir . --select +gold"
        ),
    )

    dbt_run >> dbt_test
