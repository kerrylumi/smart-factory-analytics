-- Loob bronze/silver/gold skeemid ja bronze raw tabelid.
-- See fail aktiveerub automaatselt esimesel db-konteineri käivitusel
-- AINULT siis, kui compose.yml-i `db` teenusel on volume:
--     ./init:/docker-entrypoint-initdb.d
-- Praegu pole see volume veel lisatud — DAG ise tagab CREATE IF NOT EXISTS.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Töövoo käivituste jälgimine
CREATE TABLE IF NOT EXISTS bronze.pipeline_runs (
    run_id        uuid        PRIMARY KEY,
    fetched_at    timestamptz NOT NULL,
    source_name   text        NOT NULL,
    status        text        NOT NULL,  -- 'running' | 'success' | 'failed'
    message       text
);

-- Elering NPS toorandmed (15-min lahutus, EUR/MWh)
CREATE TABLE IF NOT EXISTS bronze.br_electricity_prices (
    run_id         uuid          NOT NULL REFERENCES bronze.pipeline_runs(run_id),
    country        text          NOT NULL,        -- 'ee' | 'fi' | 'lv' | 'lt'
    ts_utc         timestamptz   NOT NULL,        -- 15-min vahemiku algus UTC-s
    price_eur_mwh  numeric(10,4) NOT NULL,
    loaded_at      timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (country, ts_utc)
);
