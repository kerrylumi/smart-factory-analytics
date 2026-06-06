# Edenemisraport — Sprint 2 (25.05–31.05)

## Mis on valmis

- [x] Docker Compose käivitab kõik teenused (pgDuckDB, HiveMQ, MQTT simulaator, Redpanda Connect, Airflow 3.1.8, dbt, Superset 6.0, Jupyter/PySpark)
- [x] Andmeid saadakse mõlemast allikast kätte:
  - Elering NPS API (15-min lahutusega päevahinnad EE/FI/LV/LT)
  - `metalfab-simulator` MQTT telemeetria HiveMQ kaudu (PackML olekud + sensorid, ~5s)
- [x] Andmed laetakse `bronze` kihti (`bronze.br_electricity_prices` + audit `bronze.pipeline_runs`, ~11 800 kirjet pärast 31 päeva backfilli)
- [x] MQTT andmete sissevõtt failideks: Redpanda Connect kuulab `umh/v1/metalfab/eindhoven/+/+/_raw/#` ja kirjutab Hive-partitioned JSON-id `data/lake/year=…/month=…/day=…/dept=…/machine=…/tag=…/` puusse
- [x] Streaming töötlus: Jupyteri PySpark Structured Streaming notebook (`notebooks/metalfab-streaming.ipynb`) loeb `data/lake/`-st mikrobatch-režiimis ja kirjutab `bronze.raw_factory_data` tabelisse JDBC kaudu
- [x] Vähemalt üks transformatsioon toimib (`silver.silver_electricity_prices` view: UTC → Europe/Tallinn ajavöönd, EUR/MWh → EUR/kWh, NULL filter)
- [x] Vähemalt üks näidikulaud on nähtaval (Superset "Tehase juhtimislaud", 4 chart'i):
  - Elering elektrihind 15-min lahutusega (silver view'st)
  - Elering elektrihind päevase keskmisena (silver view'st)
  - Tükitoodang masina kohta — bar chart `bronze.raw_factory_data` pealt (`MAX(value)` kus `tag='parts_produced'`)
  - Tehase üldine energiatarbimine ajas — line chart `bronze.raw_factory_data` pealt (`machine='main'`, `tag IN ('consumption_kw','grid_import_kw','solar_generation_kw')`)
- [x] Vähemalt üks andmekvaliteedi test läbib (4 dbt testi: `ts_utc` not_null + unique, `price_eur_mwh` ja `price_eur_kwh` not_null)

**Detailid:**
- DAG `elering_ingest` (`dags/elering_ingest.py`) — `@daily` schedule, 4 task'i (`ensure_schema >> laadi_hinnad >> dbt_run >> dbt_test`), idempotent ON CONFLICT DO NOTHING
- Redpanda Connect konfig `config/redpanda-connect.yaml` — Bloblang mapping parsib UNS topic'u (`dept`/`machine`/`tag`), output kirjutab failidena `data/lake/`-i
- Spark streaming notebook — `foreachBatch` JDBC kirjutus `bronze.raw_factory_data` tabelisse, checkpoint `./checkpoints/postgres_stream`
- dbt projekt `dbt_project/` — sources, silver mudel, schema-yml testidega; custom `generate_schema_name` macro suunab `+schema:` väärtuse otse skeemi nimeks
- Init SQL `init/01_create_schemas.sql` — bronze/silver/gold skeemid; volume veel kommenteeritud, DAG ise tagab CREATE IF NOT EXISTS
- Superset compose'is — `Dockerfile.superset`, `superset/superset_config.py`, dashboard ZIP `superset/dashboards/elering_dashboard.zip`
- `.env.example` kasutusvalmis mall, päris `.env` on `.gitignore`-s

## Järgmised sammud (Sprint 3)

- **MQTT bronze tabel dbt-sse:** `bronze.raw_factory_data` (kuhu Spark juba kirjutab) lisada `sources.yml`-i; ehitada silver-kihi view'd (PackML olekute kestus, tükiloendurite delta, sensoriaegrida masinate kaupa). Pärast seda suunata dashboardi tükitoodangu ja energiatarbimise chart'id silver-kihi peale — praegu päringud käivad otse bronze tabelilt, mis on lühiajaline tehniline võlg.
- **`seeds/`-kataloog luua:** `masinad.csv` ja `toote_info.csv` (arhitektuuris kirjeldatud, kuid faile veel pole)
- **dbt gold-kiht:** OEE arvutused (Running/Idle/Fault olekud + tükiloendurid), energiakulu × elektrihind tunniagregaadid, downtime cost mudel
- **Dashboard'i laiendamine** täis-KPI komplektiga (OEE, tootmisühiku energiakulu, seisuaja kulu) + dbt testide laiendamine bronze ja gold kihile
- README täielik versioon kursuse malli järgi

---

# Edenemisraport — Sprint 3 (01.06–06.06)

## Mis on valmis

- [x] **MQTT bronze tabel dbt-source'ina:** `bronze.raw_factory_data` registreeritud `sources.yml`-i; silver-view `silver_factory_telemetry` puhastab telemeetria long-formaadis (NULL-filter + Europe/Tallinn ajatempel)
- [x] **dbt gold-kiht valmis** — star-skeemi tabelid masinate toorsignaalidest (mitte simulaatori eelarvutatud `oee.*` tag'idest):
  - `gold_oee_availability` — Availability = run_time / planned_time, jooksev kumulatiivne minuti kaupa
  - `gold_oee_performance` — Performance = toodetud / (run_h × ideaalmäär); ideaalmäär seemnest `ideal_cycle_rates`
  - `gold_oee_quality` — Quality = (toodetud − praak) / toodetud, loendurite delta kumulatiivina
  - `gold_oee` — liit-OEE = Availability × Performance × Quality, live-trend minuti kaupa
  - `gold_energy` + `gold_energy_per_part` — energiakulu Eleringi REAALSETE spot-hindadega (€ ja €/tükk)
  - `gold_downtime` — seisakute jaotus PackML olekute kaupa, minuti kaupa
- [x] **Seemnefail** `seeds/ideal_cycle_rates.csv` — masinatüübi ideaalne tootmiskiirus (tükki/h), OEE Performance baas
- [x] **Gold-kihi testid** — schema-yml not_null testid kõigil mudelitel + 2 singular-testi (`assert_oee_komponendid_0_1`: OEE komponendid vahemikus 0–1; `assert_energy_grid_le_gross`: võrgu-import ≤ kogutarve)
- [x] **Automaatne gold-uuendus** — uus DAG `dbt_gold_refresh` (`dags/dbt_gold_refresh.py`), `*/5 * * * *`, käivitab `dbt run + test --select +gold` (ehitab ka upstream silver view'd)

**Detailid ja parandused:**
- OEE grain on **minut** ja väärtused **jooksvalt kumulatiivsed** (running window) → sile, iga minut muutuv live-OEE trend, nagu päris dashboardidel
- Leitud ja parandatud 2 andmeviga, mille uus striimitud andmestik paljastas:
  - **Andmeauk:** ~5-päevane auk andmetes (05-31 → 06-04) tekitas LEAD-iga võltsi mitmepäevase state-intervalli, mis paisutas run-time'i. Lahendus: augu-kaitse — >120s intervall = puuduv andmestik, ignoreeritakse (OEE + energia mudelis)
  - **grid > consumption öösel:** proovivõtu jitter ristas 15-min integraalid → negatiivne päikese-sääst. Lahendus: `grid_import` klambritud ≤ `consumption` (füüsiline korrektsus)
- Gold uueneb iseseisvalt Elering DAG-ist, sest OEE sõltub tehase telemeetriast, mitte elektrihinnast

## Kokkuvõte

Lahendus on terviklik: mõlemad sissevõtud, silver-kiht ja kogu gold-kihi KPI-komplekt (OEE, energiakulu, downtime) töötavad otsast lõpuni, testitud ja automaatselt uuenevad. Läheme edasi selle valminud lahendusega.

## Mis takistab

- Praegu pole blokeerivaid probleeme.
- Sprint 3 alguses tasub kokku leppida, kas Spark notebook jääb streaming pipeline'i osaks pikemaajaliselt, või konsolideerime kogu sissevõtu Redpanda Connect'i alla (Redpanda Connect oskab ka otse Postgresisse kirjutada — eemaldaks Sparki sõltuvuse).

## Kontrollpunkt

Käsud, millega saab kontrollida, et töövoog töötab:

```bash
# 1. Käivita stack (esmakordsel käivitusel ka --build)
docker compose up -d

# 2. Käivita Elering DAG ja tee backfill soovitud perioodile (muuda from-date ja to-date parameetreid vastavalt soovitud backfill perioodile)
#    (Airflow 3: `airflow backfill create`)
docker compose exec airflow-scheduler airflow dags unpause elering_ingest
docker compose exec airflow-scheduler airflow backfill create \
  --dag-id elering_ingest \
  --from-date 2026-05-01 \
  --to-date 2026-05-31 \
  --max-active-runs 1

# 3. Kontrolli, et MQTT pipeline kirjutab faile (peaks tekkima minutite jooksul)
find data/lake -name "*.json" | head -5
find data/lake -name "*.json" | wc -l

# 4. Kontrolli bronze tabel (Elering)
docker compose exec db psql -U praktikum -d praktikum \
  -c "SELECT count(*) AS rows, count(DISTINCT country) AS countries FROM bronze.br_electricity_prices;"

# 5. Kontrolli silver view
docker compose exec db psql -U praktikum -d praktikum \
  -c "SELECT count(*), min(ts_eest), max(ts_eest) FROM silver.silver_electricity_prices;"

# 6. Käivita Jupyteri notebook ja kontrolli MQTT bronze tabel
#    http://localhost:8888  (token: praktikum) → notebooks/metalfab-streaming.ipynb → Run All
docker compose exec db psql -U praktikum -d praktikum \
  -c "SELECT count(*), count(DISTINCT machine) FROM bronze.raw_factory_data;"

# 7. Ava Superset → näidikulaud
#    http://localhost:8088  (admin / admin)
```

**Oodatav tulemus pärast backfill'i ja MQTT pipeline'i lühiajalist tööd:**
- `bronze.br_electricity_prices` täitub ~88–96 kirjega päeva ja riigi kohta (4 riiki); näiteks 30 päeva backfill annab ~11 000 kirjet. Silver view tagastab Eesti read EUR/MWh + EUR/kWh veerus, Superset dashboard kuvab elektrihinna ajagraafiku.
- `data/lake/` puus tekib esimeste minutite jooksul tuhandeid JSON-faile (simulaator 5s tsükkel × masinate ja tagide arv).
- Pärast Spark notebook'i käivitamist täitub `bronze.raw_factory_data` esimeste batch'idega; iga uus mikrobatch lisab uusi ridu.
