# Smart Factory Analytics

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
| `seeds/ideal_cycle_rates.csv` | dbt seed | Ei | Masinatüübi ideaalne tootmiskiirus (tükki/h) — OEE Performance baasmäär |

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

# 4. Laadi dbt seemned (ideal_cycle_rates — gold OEE Performance baasmäär)
#    KOHUSTUSLIK enne gold-kihti: gold_oee_performance teeb ref('ideal_cycle_rates').
#    Seeme on staatiline, nii et piisab ühest korrast. dbt run/build EI laadi seemneid.
docker compose run --rm dbt -lc "dbt seed --profiles-dir ."

# 5. Käivita Elering DAG ja tee backfill (muuda from-date/to-date oma perioodile)
#    (Airflow 3: `airflow backfill create`, mitte enam `dags backfill`)
#    NB! Vali periood, mis KATTUB tehase telemeetria ajaga (vt samm 6 — andmed on "praegu"),
#    muidu gold_energy (hind × tarve INNER-join 15-min plokis) jääb tühjaks.
docker compose exec airflow-scheduler airflow dags unpause elering_ingest
docker compose exec airflow-scheduler airflow backfill create \
  --dag-id elering_ingest \
  --from-date 2026-06-01 \
  --to-date 2026-06-06 \
  --max-active-runs 1

# 6. Käivita MQTT-poole streaming
#    Ava http://localhost:8888 → notebooks/metalfab-streaming.ipynb → Run All
#    (Redpanda Connect alustab faili kirjutamist data/lake/-i automaatselt)
#    NB! Notebooki lugemis-glob on hard-coded jooksvale kuule (data/lake/*/month=06/...).
#        Teises kuus muuda notebookis `month=06` vastavaks (nt month=07).

# 7. Aktiveeri gold-kihi automaatne uuendus (OEE, energia, downtime iga 5 min)
docker compose exec airflow-scheduler airflow dags unpause dbt_gold_refresh
```

### Superset dashboardi taastamine backupist

Dashboardi ZIP-eksport on versioneeritud repos (`superset/dashboards/dashboard.zip`). Värskes keskkonnas (esimene `docker compose up` või pärast `superset-db` volume'i kustutamist) tuleb dashboard UI kaudu tagasi laadida:

1. Ava http://localhost:8088, logi sisse (`admin` / `admin`).
2. Settings (paremas ülanurgas) → **Import Dashboards**.
3. Vali fail `superset/dashboards/dashboard.zip`. Kui küsitakse, sisesta `SECRET_KEY` `.env`-st (sama `SUPERSET_SECRET_KEY`, millega ZIP eksporditi).
4. Pärast importi: **Datasets** sakil kontrolli, et dashboardi datasetid (`silver_electricity_prices` ning gold-KPI-d `gold_oee`, `gold_downtime`, `gold_energy` jt) on seotud `praktikum` andmebaasiga (ühendus `pgDuckDB`). Kui ühendust pole, lisa see: **Settings → Database Connections → + Database → PostgreSQL**, URI `postgresql://praktikum:praktikum@db:5432/praktikum`.

**Backupi uuendamine:** kui dashboardi muudad ja muutused soovid repo'sse panna, mine **Dashboards** loendisse → kolm täppi (`⋯`) dashboardi real → **Export → Export as ZIP** ja kirjuta `superset/dashboards/dashboard.zip` üle.

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

### Lahenduse verifitseerimine

Pärast sammude 1–7 läbimist kontrolli, et iga kiht on andmetega täitunud. Käsud kihi kaupa (bronze → silver → gold):

```bash
# Bronze — Elering hinnad (peaks olema sadu ridu päeva ja riigi kohta)
docker compose exec db psql -U praktikum -d praktikum \
  -c "SELECT count(*) AS read, count(DISTINCT country) AS riike FROM bronze.br_electricity_prices;"

# Bronze — tehase telemeetria (täitub pärast notebooki käivitamist)
docker compose exec db psql -U praktikum -d praktikum \
  -c "SELECT count(*) AS read, count(DISTINCT machine) AS masinaid FROM bronze.raw_factory_data;"

# Silver — view'd (peegeldavad live bronze'i; tagastavad ridu kohe)
docker compose exec db psql -U praktikum -d praktikum \
  -c "SELECT count(*) FROM silver.silver_factory_telemetry;"

# Gold — OEE (jooksev kumulatiivne minuti kaupa; uueneb iga 5 min DAG-iga)
docker compose exec db psql -U praktikum -d praktikum \
  -c "SELECT count(*) AS read, max(minut) AS viimane_minut FROM gold.gold_oee;"

# Gold — energiakulu (TÜHI, kui Elering periood ei kattu telemeetria ajaga, vt samm 5)
docker compose exec db psql -U praktikum -d praktikum \
  -c "SELECT count(*), min(ts_utc), max(ts_utc) FROM gold.gold_energy;"

# Gold — seisakute jaotus
docker compose exec db psql -U praktikum -d praktikum \
  -c "SELECT count(*) FROM gold.gold_downtime;"
```

**Oodatav tulemus:**
- `bronze.br_electricity_prices` — ~88–96 rida päeva × riigi kohta (4 riiki).
- `bronze.raw_factory_data` — kasvab iga Spark-mikrobatch'iga; `masinaid` > 0.
- `gold.gold_oee` — ridu mitu (masin × minut), `viimane_minut` lähedal praegusele ajale.
- `gold.gold_energy` — read > 0 **ainult siis**, kui Elering backfill ja telemeetria perioodid kattuvad.

Kõik dbt-testid peavad olema rohelised:

```bash
docker compose run --rm dbt -lc "dbt test --profiles-dir . --select silver gold"
```

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
4. **Transformatsioon — silver** — dbt teisendab bronze andmed silver-kihi view'desse: `silver_electricity_prices` (UTC → Europe/Tallinn, EUR/MWh → EUR/kWh) ja `silver_factory_telemetry` (tehase telemeetria long-formaadis, NULL-filter + Tallinna ajatempel).
5. **Transformatsioon — gold** — gold-kihi star-skeemi tabelid arvutatakse masinate toorsignaalidest: **OEE** minuti kaupa jooksva kumulatiivina (`gold_oee` = Availability × Performance × Quality), **energiakulu** Eleringi reaalsete spot-hindadega (`gold_energy`, `gold_energy_per_part`) ja **seisakute jaotus** PackML olekute kaupa (`gold_downtime`). Gold uueneb automaatselt iga 5 min DAG-iga `dbt_gold_refresh`; silver on view'd, mis peegeldavad alati live bronze andmeid.
6. **Testimine** — dbt schema-yml + singular-testid kontrollivad silver- ja gold-kihi võtmevälju ja loogikat (vt allpool).
7. **Näidikulaud** — Superset kuvab elektrihinna 15-min lahutusel ja päevase keskmisena ning gold-kihi KPI-d: live-OEE masina kohta (minuti kaupa), €/tükk ja seisakute breakdown.

## Andmekvaliteedi testid

**Silver** (`dbt_project/models/silver/schema.yml`) — `silver_electricity_prices`:

1. `ts_utc` on **not_null** ja **unique** (üks rida 15-min ajaakna kohta)
2. `ts_eest` on **not_null** (ajavööndi teisendus ei tohi NULL-i luua)
3. `price_eur_mwh` on **not_null**
4. `price_eur_kwh` on **not_null** (matemaatika kontroll)

`silver_factory_telemetry` — `machine`, `tag`, `value`, `ts_utc` on **not_null**.

**Gold** (`dbt_project/models/gold/schema.yml`) — schema-testid kontrollivad iga OEE-mudeli (`gold_oee` + komponendid) võtmevälju `machine`, `minut` ja KPI-veerge (`availability`, `performance`, `quality`, `oee`) **not_null**, samuti `gold_downtime`, `gold_energy` ja `gold_energy_per_part` võtmevälju (`ts_utc` not_null + unique jne).

**Singular-testid** (`dbt_project/tests/`) kontrollivad ärireegleid:
- `assert_oee_komponendid_0_1.sql` — kõik OEE komponendid jäävad vahemikku 0–1
- `assert_energy_grid_le_gross.sql` — võrgust imporditud energia ei ületa kogutarvet (füüsiline korrektsus)

Testide käivitamine ja vaatamine:

```bash
docker compose run --rm dbt -lc "dbt test --profiles-dir . --select silver"   # silver
docker compose run --rm dbt -lc "dbt test --profiles-dir . --select gold"     # gold
```

Airflow käivitab testid pärast iga laadimist: `elering_ingest` testib silver-kihti, `dbt_gold_refresh` (iga 5 min) testib gold-kihti. Punase task'i puhul on testidetailid Airflow UI logides.

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
│   ├── elering_ingest.py           ← Elering NPS @daily DAG (ensure_schema → laadi → dbt run → test silver)
│   └── dbt_gold_refresh.py         ← Gold-kihi uuendus iga 5 min (dbt run + test --select +gold)
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/generate_schema_name.sql
│   ├── seeds/ideal_cycle_rates.csv ← Masinatüübi ideaalmäär (tükki/h), OEE Performance baas
│   ├── tests/                      ← Singular-testid (OEE 0–1, energia grid ≤ gross)
│   └── models/
│       ├── silver/                 ← Silver view'd (electricity + factory telemetry) + sources + testid
│       └── gold/                   ← Gold star-skeem: OEE (4 mudelit), energia (2), downtime + testid
├── init/
│   └── 01_create_schemas.sql       ← bronze/silver/gold skeemid (volume veel mountimata)
├── notebooks/
│   └── metalfab-streaming.ipynb    ← PySpark Structured Streaming → bronze.raw_factory_data
├── superset/
│   ├── superset_config.py
│   └── dashboards/dashboard.zip
├── metalfab-simulator/             ← Vendored MQTT andmegeneraator (Python pakett)
├── docs/
│   ├── arhitektuur.md              ← Nädal 1 väljund
│   └── progress.md                 ← Nädalapõhine edenemisraport
├── data/lake/                      ← Redpanda Connect kirjutab siia (gitignore'is)
└── logs/                           ← Airflow logid (gitignore'is)
```

## Kokkuvõte ja puudused

**Kokkuvõte:**

- Mõlemad sissevõtud (Elering REST + MQTT streaming) töötavad otsast lõpuni. Elering kirjutab `bronze.br_electricity_prices` tabelisse ja silver-view tarnib Superseti dashboardile; MQTT andmed jõuavad `data/lake/`-st Sparki kaudu `bronze.raw_factory_data` tabelisse.
- `bronze.raw_factory_data` on registreeritud dbt-source'ina ja silver-view `silver_factory_telemetry` puhastab telemeetria gold-kihi jaoks.
- Gold-kiht on valmis: OEE (Availability × Performance × Quality) arvutatakse masinate toorsignaalidest jooksva kumulatiivina minuti kaupa, energiakulu Eleringi reaalsete spot-hindadega ja seisakute jaotus PackML olekute kaupa. Kõik gold-mudelid on testitud (schema + singular testid).
- dbt projekt, testid ja Superset dashboard on käivitatavad ühe `docker compose up -d` käsuga.
- Orkestreerimine: `elering_ingest` (@daily) hoiab bronze + silver elektrihinna värske, `dbt_gold_refresh` (iga 5 min) hoiab gold-KPI-d värske. Mõlemad idempotentsed (`ON CONFLICT DO NOTHING`), nii et backfill on ohutu.

**Puudused:**

- Kõiki äriküsimusi ei ole dashboardil päris sellisel kujul esitatud, nagu algselt eeldada võiks — osa mõõdikuid on kättesaadav gold-kihis, kuid pole veel täielikult Superseti visualiseeritud.
- Tootmisühiku energiakulu toote kohta (€/tükk masina lõikes) ei õnnestunud päris realiseerida, sest simulaator ei väljastanud **masinapõhist** energiatarvet — energiatarve oli saadaval ainult **summaarsena kogu tehase kohta**. Seetõttu on energiakulu arvutatud tehase tasemel, mitte üksikmasina/toote täpsusega.
- Andmekvaliteedi kontrollid ei pruugi olla kõige tõhusamad — praegu keskenduvad need peamiselt `not_null`/`unique` ja väärtusvahemike testidele, mitte sügavamale ärireeglite valideerimisele.

## Meeskond

| Nimi | Roll |
|------|------|
| Kerry Lumi | Metalfab MQTT omanik; transformatsioonid (kaasvastutus) |
| Erki Ohmann | Elering API omanik; transformatsioonid (kaasvastutus) |
| Kärt Kesküla | Andmekvaliteet (testid) ja näidikulaud |
