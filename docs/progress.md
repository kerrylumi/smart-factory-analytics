# Edenemisraport — Sprint 2 (25.05–31.05)

## Mis on valmis

- [x] Docker Compose käivitab kõik teenused (pgDuckDB, HiveMQ, MQTT simulaator, Airflow 3.1.8, dbt, Superset 6.0)
- [x] Andmeid saadakse allikast kätte (Elering NPS API, 15-min lahutusega päevahinnad EE/FI/LV/LT)
- [x] Andmed laetakse `bronze` kihti (`bronze.br_electricity_prices` + audit `bronze.pipeline_runs`, ~11 800 kirjet pärast 31 päeva backfilli)
- [x] Vähemalt üks transformatsioon toimib (`silver.silver_electricity_prices` view: UTC → Europe/Tallinn ajavöönd, EUR/MWh → EUR/kWh, NULL filter)
- [x] Vähemalt üks näidikulaud on nähtaval (Superset "Tehase juhtimislaud": kaks chart'i — 15-min lahutus + päevane keskmine)
- [x] Vähemalt üks andmekvaliteedi test läbib (4 dbt testi: `ts_utc` not_null + unique, `price_eur_mwh` ja `price_eur_kwh` not_null)

**Detailid:**
- DAG `elering_ingest` (`dags/elering_ingest.py`) — `@daily` schedule, 4 task'i (`ensure_schema >> laadi_hinnad >> dbt_run >> dbt_test`), idempotent ON CONFLICT DO NOTHING
- dbt projekt `dbt_project/` — sources, silver mudel, schema-yml testidega
- Init SQL `init/01_create_schemas.sql` — bronze/silver/gold skeemid + tabelid auto-luuakse uue pgDuckDB volume'i puhul
- Superset compose'is — `Dockerfile.superset`, `superset/superset_config.py`, dashboard ZIP `superset/dashboards/elering_dashboard.zip`
- `.env.example` kasutusvalmis mall, päris `.env` on `.gitignore`-s

## Järgmised sammud (Sprint 3)

- MQTT-pool: Benthos `data/lake` → bronze tabel
- dbt gold-kiht (OEE arvutused, energiakulu × elektrihind agregeerimine)
- Dashboard'i laiendamine täis-KPI komplektiga + dbt testide laiendamine
- README täielik versioon kursuse malli järgi

## Mis takistab

- Praegu pole blokeerivaid probleeme. `arhitektuur.md` sõnastust "tunnipõhine" uuendada Sprint 3 ajal (tegelik andmesagedus 15-min, NPS turg liikus 2025-st 15-min lahutusele).

## Kontrollpunkt

Käsk, millega saab kontrollida, et töövoog töötab:

```bash
# 1. Käivita stack (esmakordsel käivitusel ka --build)
docker compose up -d

# 2. Käivita Elering DAG (1x päevaks või backfill mitme päeva jaoks)
docker compose exec airflow-scheduler airflow dags unpause elering_ingest
docker compose exec airflow-scheduler airflow dags trigger elering_ingest

# 3. Kontrolli bronze tabel
docker compose exec db psql -U praktikum -d praktikum \
  -c "SELECT count(*) AS rows, count(DISTINCT country) AS countries FROM bronze.br_electricity_prices;"

# 4. Kontrolli silver view
docker compose exec db psql -U praktikum -d praktikum \
  -c "SELECT count(*), min(ts_eest), max(ts_eest) FROM silver.silver_electricity_prices;"

# 5. Ava Superset → näidikulaud
#    http://localhost:8088  (admin / admin)
```

**Oodatav tulemus pärast üksikut DAG-i käivitust:** `bronze.br_electricity_prices` täitub ~88–96 kirjega riigi kohta (4 riiki), silver view tagastab Eesti read EUR/MWh + EUR/kWh veerus, Superset dashboard kuvab elektrihinna ajagraafiku.
