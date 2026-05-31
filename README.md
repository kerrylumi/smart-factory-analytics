# Grupp 7 — Smart Factory Analytics

Metallitööstuse tehase reaalaja telemeetria ja elektri börsihinna ühendamine OEE-ja kuluanalüüsiks.

## Äriküsimus

Kuidas masinate seisuajad ja elektrihinna kõikumised mõjutavad toodangu omahinda ja seadmete üldist efektiivsust (OEE)? Lahendus aitab tootmisjuhil näha, millal tootmine on rahaliselt kahjumlik (elektri börsihind ületab toote katte) ja kus seisuajad maksavad kõige rohkem.

**Mõõdikud:**

1. **OEE (Overall Equipment Effectiveness)** — arvutatud reaalajas masina olekute (Running / Idle / Fault) ja tükitoodangu põhjal
2. **Tootmisühiku energiakulu (€)** — masina võimsustarbimine (kW) × Elering NPS börsihind (€/MWh)
3. **Seisuaja kulu (Downtime Cost)** — plaaniväliste seisakute rahaline mõju
4. **Tootmise tasuvuse tagantjärele analüüs** — kogu kahjum tundidel, mil elektrihind tegi omahinna kõrgemaks kui müügihind

## Arhitektuur

```mermaid
flowchart LR
    subgraph Allikad
        SIM[metalfab-simulator<br/>PackML + telemeetria]
        EL[Elering NPS API<br/>börsihind]
    end

    subgraph Sissevõtt
        SIM -->|MQTT| HM[HiveMQ CE]
        HM --> RPC[Redpanda Connect<br/>JSON write]
        EL -->|HTTPS| ING[Airflow DAG<br/>elering_ingest]
    end

    RPC --> LAKE[(data/lake<br/>Hive-partitioned JSON)]
    LAKE --> SPK[Jupyter PySpark<br/>Structured Streaming]
    SPK --> STG[(bronze.raw_factory_data)]
    ING --> BR[(bronze<br/>br_electricity_prices)]
    STG --> SLV[(silver<br/>view'd)]
    BR --> SLV
    SLV --> GLD[(gold<br/>star-skeem)]
    GLD --> SUP[Superset]

    AF[Airflow<br/>dbt run + test] -.orkestreerib.-> BR
    AF -.orkestreerib.-> SLV
    AF -.orkestreerib.-> GLD
```

Täpsem kirjeldus: [`docs/arhitektuur.md`](docs/arhitektuur.md). Jooksev edenemine: [`docs/progress.md`](docs/progress.md).

## Andmestik

| Allikas | Tüüp | Ajas muutuv? | Roll |
|---------|------|--------------|------|
| `metalfab-simulator` (Eindhoven, Level 4) | MQTT | Jah, ~5s | Masina sensorid, olekud, tükiloendurid |
| Elering NPS API | HTTPS JSON | Jah, 15-min lahutus, ajaloolised päringuid | Elektri börsihind €/MWh (EE/FI/LV/LT) |
| `seeds/masinad.csv` *(Sprint 3)* | dbt seed | Ei | Masinate metaandmed |
| `seeds/toote_info.csv` *(Sprint 3)* | dbt seed | Ei | Toote metaandmed |

## Stack

| Komponent | Tööriist |
|-----------|---------|
| MQTT broker | HiveMQ CE |
| MQTT andmegeneraator | metalfab-uns-simulator (Python) |
| Streaming sissevõtt | Redpanda Connect (Bloblang) |
| Batch sissevõtt | Apache Airflow 3.1.8 (Python/psycopg2) |
| Streaming töötlus | Apache Spark 4 / PySpark Structured Streaming (Jupyter) |
| Transformatsioon | dbt-postgres 1.10 |
| Andmehoidla | pgDuckDB (PostgreSQL 18 + DuckDB) |
| Näidikulaud | Apache Superset 6.0 |
| Orkestreerimine | Apache Airflow |

## Käivitamine

```bash
# 1. Klooni repo ja liigu kausta
git clone <repo-url>
cd smart-factory-analytics

# 2. Kopeeri keskkonnamuutujad ja muuda vajaduspõhiselt
cp .env.example .env
#    Pordid ja paroolid arendusvaikimisi väärtustega — vt allpool "Saladused ja konfiguratsioon".

# 3. Käivita kõik teenused
docker compose up -d --build

# 4. Käivita Elering DAG ja tee backfill soovitud perioodile (muuda from-date ja to-date parameetreid vastavalt soovitud backfill perioodile)
#    (Airflow 3: `airflow backfill create`, mitte enam `dags backfill`)
docker compose exec airflow-scheduler airflow dags unpause elering_ingest
docker compose exec airflow-scheduler airflow backfill create \
  --dag-id elering_ingest \
  --from-date 2026-05-01 \
  --to-date 2026-05-31 \
  --max-active-runs 1
# 5. Käivita MQTT-poole streaming
#    Ava http://localhost:8888 → notebooks/metalfab-streaming.ipynb → Run All
#    (Redpanda Connect alustab faili kirjutamist data/lake/-i automaatselt)
```

### Superset dashboardi taastamine backupist

Dashboardi ZIP-eksport on versioneeritud repos (`superset/dashboards/elering_dashboard.zip`). Värskes keskkonnas (esimene `docker compose up` või pärast `superset-db` volume'i kustutamist) tuleb dashboard UI kaudu tagasi laadida:

1. Ava http://localhost:8088, logi sisse (`admin` / `admin`).
2. Settings (paremas ülanurgas) → **Import Dashboards**.
3. Vali fail `superset/dashboards/elering_dashboard.zip`. Kui küsitakse, sisesta `SECRET_KEY` `.env`-st (sama `SUPERSET_SECRET_KEY`, millega ZIP eksporditi).
4. Pärast importi: **Datasets** sakil kontrolli, et `silver_electricity_prices` dataset on seotud `praktikum` andmebaasiga (ühendus `pgDuckDB`). Kui ühendust pole, lisa see: **Settings → Database Connections → + Database → PostgreSQL**, URI `postgresql://praktikum:praktikum@db:5432/praktikum`.

**Backupi uuendamine:** kui dashboardi muudad ja muutused soovid repo'sse panna, mine **Dashboards** loendisse → kolm täppi (`⋯`) elering dashboardi real → **Export → Export as ZIP** ja kirjuta `superset/dashboards/elering_dashboard.zip` üle.

### Teenused ja pordid

| Teenus | Konteiner | Host port | Sisemine port | Kasutaja / märkused |
|--------|-----------|-----------|---------------|---------------------|
| HiveMQ CE (MQTT broker) | `hivemq` | **1883** (MQTT), **8083** (WebSocket) | 1883, 8000 | Anonüümne ligipääs |
| metalfab-simulator | `metalfab-simulator` | — | — | Pole UI-d, publitseerib MQTT-le |
| Redpanda Connect | `redpanda-connect` | — | — | Pole UI-d, kirjutab faile `data/lake/`-i |
| pgDuckDB (analytics) | `metalfab-db` | **5432** | 5432 | `praktikum` / `praktikum` (vt `.env`) |
| dbt (CLI konteiner) | `metalfab-dbt` | — | — | `docker compose exec dbt bash` |
| Airflow API + UI | `metalfab-airflow-api` | **8080** | 8080 | `airflow` / `airflow` |
| Airflow scheduler | `metalfab-airflow-scheduler` | — | — | Töötab taustal |
| Airflow DAG processor | `metalfab-airflow-dagproc` | — | — | Töötab taustal |
| Airflow metadata DB | `metalfab-airflow-db` | — | 5432 | Eraldatud `airflow-db-volume`-is |
| Superset | `metalfab-superset` | **8088** *(seadistatav `SUPERSET_PORT_HOST` kaudu)* | 8088 | `admin` / `admin` |
| Superset metadata DB | `metalfab-superset-db` | — | 5432 | Eraldatud `superset-db-volume`-is |
| Jupyter (PySpark) | `metalfab-jupyter` | **8888** (UI), **4040** (Spark UI) | 8888, 4040 | Token `praktikum` (vt `JUPYTER_TOKEN`) |

**URL-id kiireks viiteks:**
- Airflow UI — http://localhost:8080
- Superset dashboard — http://localhost:8088
- Jupyter Lab — http://localhost:8888 (token: `praktikum`)
- HiveMQ WebSocket — ws://localhost:8083
- pgDuckDB — `postgresql://praktikum:praktikum@localhost:5432/praktikum`



## Saladused ja konfiguratsioon

Kõik saladused on `.env` failis, mis on `.gitignore`-s. Mall on `.env.example` (kõik vajalikud muutujad arendusvaikimisi väärtustega): kopeeri `cp .env.example .env` ja muuda väärtused vastavalt vajadusele. Kõik muutujad:

| Muutuja | Tähendus | Näiteväärtus |
|---------|----------|--------------|
| `POSTGRES_USER` | pgDuckDB kasutaja (analytics) | `praktikum` |
| `POSTGRES_PASSWORD` | pgDuckDB parool | `praktikum` |
| `POSTGRES_DB` | pgDuckDB andmebaasi nimi | `praktikum` |
| `AIRFLOW_UID` | Konteineri kasutaja UID (Linuxis `id -u`, Mac/Windows: jäta `50000`) | `50000` |
| `AIRFLOW_USER` | Airflow metaandmebaasi kasutaja | `airflow` |
| `AIRFLOW_PASSWORD` | Airflow metaandmebaasi parool | `airflow` |
| `AIRFLOW_DB` | Airflow metaandmebaasi nimi | `airflow` |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Airflow API JWT allkirjastamise võti | (juhuslik string) |
| `AIRFLOW__API_AUTH__JWT_ISSUER` | JWT issuer väärtus | `airflow` |
| `_AIRFLOW_WWW_USER_USERNAME` | Airflow UI admin kasutaja | `airflow` |
| `_AIRFLOW_WWW_USER_PASSWORD` | Airflow UI admin parool | `airflow` |
| `SUPERSET_DB_USER` | Superset metaandmebaasi kasutaja | `superset` |
| `SUPERSET_DB_PASSWORD` | Superset metaandmebaasi parool | (saladus) |
| `SUPERSET_DB_NAME` | Superset metaandmebaasi nimi | `superset_meta` |
| `SUPERSET_SECRET_KEY` | Superset session-küpsiste võti (min 32 tähemärki) | `python -c "import secrets; print(secrets.token_hex(32))"` |
| `SUPERSET_ADMIN_USER` | Superset UI admin | `admin` |
| `SUPERSET_ADMIN_PASSWORD` | Superset UI admin parool | `admin` |
| `SUPERSET_ADMIN_EMAIL` | Superset admin e-post | `admin@example.com` |
| `SUPERSET_PORT_HOST` | Host port, mille kaudu Superset on kättesaadav | `8088` |
| `JUPYTER_TOKEN` *(vabatahtlik)* | Jupyteri sisselogimise token | `praktikum` |
| `DATA_DIR` *(vabatahtlik)* | Host'i tee `data/`-kausta jaoks; vaikimisi `./data/` | `./data/` |

Isikuandmeid andmestikus pole. Avalikud API-d (Elering NPS) ei nõua võtit.

## Andmevoog lühidalt

1. **Sissevõtt — Elering** — Airflow DAG `elering_ingest` (`@daily`) pärib Elering NPS REST API-st eelmise päeva 15-min hinnad neljale riigile ja inserdib otse `bronze.br_electricity_prices`-i `ON CONFLICT DO NOTHING` strateegiaga.
2. **Sissevõtt — MQTT** — `metalfab-simulator` publitseerib PackML olekuid ja telemeetriat HiveMQ teemadele kujul `umh/v1/metalfab/eindhoven/{dept}/{machine}/_raw/{tag}`. Redpanda Connect parsib topic'u Bloblang-mappinguga ja kirjutab iga sõnumi eraldi JSON-faili Hive-partitioned puusse `data/lake/year=.../month=.../day=.../dept=.../machine=.../tag=.../`.
3. **Laadimine — streaming** — Jupyteri PySpark Structured Streaming notebook (`notebooks/metalfab-streaming.ipynb`) loeb `data/lake/`-st mikrobatch-režiimis ja kirjutab JDBC kaudu `bronze.raw_factory_data` tabelisse.
4. **Transformatsioon** — dbt projekt teisendab bronze andmed silver-kihi view'desse (`silver_electricity_prices`: UTC → Europe/Tallinn, EUR/MWh → EUR/kWh). Gold-kiht (OEE, energiakulu, downtime cost star-skeemil) on Sprint 3 töö.
5. **Testimine** — dbt schema-yml testid kontrollivad silver-kihi võtmevälju (vt allpool).
6. **Näidikulaud** — Superset "Tehase juhtimislaud" kuvab elektrihinna 15-min lahutusel ja päevase keskmisena. Täis-KPI komplekt (OEE, €/tükk, downtime €) tuleb pärast gold-kihi valmimist.

## Andmekvaliteedi testid

dbt projekt (`dbt_project/models/silver/schema.yml`) kontrollib `silver_electricity_prices` mudelit:

1. `ts_utc` on **not_null** ja **unique** (üks rida 15-min ajaakna kohta)
2. `ts_eest` on **not_null** (ajavööndi teisendus ei tohi NULL-i luua)
3. `price_eur_mwh` on **not_null**
4. `price_eur_kwh` on **not_null** (matemaatika kontroll)

Testide käivitamine ja vaatamine:

```bash
docker compose run --rm dbt -lc "dbt test --profiles-dir . --select silver"
```

Airflow DAG `elering_ingest` käivitab samad testid pärast iga andmelaadimist (`dbt_test` task) — punase task'i puhul on testidetailid Airflow UI logides.

## Projekti struktuur

```
.
├── README.md
├── CLAUDE.md                       ← Juhised Claude Code'ile (kontekst, käsud, ettevaatusabinõud)
├── compose.yml                     ← Kõikide teenuste orkestratsioon
├── Dockerfile.dbt                  ← dbt-postgres 1.10 image
├── Dockerfile.superset             ← Superset 6.0 + psycopg2-binary
├── .env                            ← Saladused (gitignore'is)
├── .gitignore
├── config/
│   ├── airflow.cfg                 ← Airflow konfiguratsioon
│   └── redpanda-connect.yaml       ← MQTT → data/lake Bloblang mapping
├── dags/
│   └── elering_ingest.py           ← Elering NPS @daily DAG (ensure_schema → laadi → dbt run → test)
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/generate_schema_name.sql
│   └── models/silver/              ← Silver view + sources + schema testid
├── init/
│   └── 01_create_schemas.sql       ← bronze/silver/gold skeemid (volume veel mountimata)
├── notebooks/
│   └── metalfab-streaming.ipynb    ← PySpark Structured Streaming → bronze.raw_factory_data
├── superset/
│   ├── superset_config.py
│   └── dashboards/elering_dashboard.zip
├── metalfab-simulator/             ← Vendored MQTT andmegeneraator (Python pakett)
├── docs/
│   ├── arhitektuur.md              ← Nädal 1 väljund
│   └── progress.md                 ← Nädalapõhine edenemisraport
├── data/lake/                      ← Redpanda Connect kirjutab siia (gitignore'is)
└── logs/                           ← Airflow logid (gitignore'is)
```

## Kokkuvõte, puudused ja võimalikud edasiarendused

**Kokkuvõte:**
- Mõlemad sissevõtud (Elering REST + MQTT streaming) töötavad otsast lõpuni. Elering kirjutab `bronze.br_electricity_prices` tabelisse ja silver-view tarnib Superseti dashboardile; MQTT andmed jõuavad `data/lake/`-st Sparki kaudu `bronze.raw_factory_data` tabelisse.
- dbt projekt, schema-yml testid ja Superset dashboard on käivitatavad ühe `docker compose up -d` käsuga.
- Airflow DAG on idempotent (`ON CONFLICT DO NOTHING`), nii et backfill on ohutu.

**Puudused:**
- Gold-kiht (OEE, energiakulu, downtime cost star-skeem) on veel kirjutamata — sealt genereeritud KPI-d puuduvad ka dashboardilt.
- MQTT bronze-tabel `bronze.raw_factory_data` pole veel dbt-source'idesse registreeritud (`sources.yml`), seega silver-mudelid MQTT andmetele puuduvad.
- `seeds/masinad.csv` ja `seeds/toote_info.csv` failid puuduvad (arhitektuuris kirjeldatud, kuid `dbt_project/seeds/` kataloogi pole loodud).

**Mis edasi:**
- `bronze.raw_factory_data` registreerimine dbt-source'ina + silver-mudelid MQTT andmetele (PackML olekute kestus, tükiloendurite delta jne); gold-kihi star-skeem (`fact_production`, `dim_machine`, `dim_time`, `dim_product`) ja OEE arvutus.
- Dashboardi laiendamine tootmisjuhi täisvaatega (live OEE per masin, €/tükk graafik, downtime breakdown).
- Kaaluda Spark notebook'i asendamist otse Redpanda Connect → PostgreSQL output'iga, mis kaotaks Spark-sõltuvuse ja võimaldaks streaming'u käivituda automaatselt koos compose stack'iga (mitte käsitsi Jupyterist).

## Meeskond

| Nimi | Roll |
|------|------|
| Kerry Lumi | Metalfab MQTT omanik; transformatsioonid (kaasvastutus) |
| Erki Ohmann | Elering API omanik; transformatsioonid (kaasvastutus) |
| Kärt Kesküla | Andmekvaliteet (testid) ja näidikulaud |
