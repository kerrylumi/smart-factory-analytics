-- Tehase energiakulu REAALSETE Eleringi elektrihindade põhjal.
-- Ristab kaks pipeline'i: tehase võimsus (kW) × Eleringi 15-min spot-hind (EUR/kWh).
-- EI kasuta simulaatori eelarvutatud `daily_cost_eur` tag'i — kulu arvutatakse ise.
--
-- Loogika:
--   1. consumption_kw / grid_import_kw on hetkvõimsuse näidud → aja-kaalutult kWh-ks (LEAD).
--   2. iga näit plokitakse Eleringi 15-min võrku (:00/:15/:30/:45 UTC).
--   3. kulu = kWh × selle ploki spot-hind. Arve käib VÕRGUST IMPORDI pealt; päike vähendab seda.
-- Grain: 15-min plokk → dünaamika ajas.

with voimsus as (
    select
        tag,
        ts_utc,
        value as kw,
        extract(epoch from (
            lead(ts_utc) over (partition by tag order by ts_utc) - ts_utc
        )) as kestus_s
    from {{ ref('silver_factory_telemetry') }}
    where machine = 'main'
      and tag in ('consumption_kw', 'grid_import_kw')
),

energia as (
    -- kW → kWh aja-kaalutult, agregeeritud 15-min Eleringi plokki
    select
        date_trunc('hour', ts_utc)
            + floor(extract(minute from ts_utc)::int / 15) * interval '15 minutes' as plokk_utc,
        tag,
        sum(kw * kestus_s / 3600.0) as kwh
    from voimsus
    where kestus_s is not null
      -- ignoreeri andmeauke: >120s intervall pole tegelik mõõtmissamm
      and kestus_s <= 120
    group by 1, 2
),

kwh_wide as (
    select
        plokk_utc,
        sum(kwh) filter (where tag = 'consumption_kw')                                   as consumption_kwh,
        -- klambrime grid <= consumption: import ei saa füüsiliselt ületada tarvet
        -- (proovivõtu jitter võib integraalid ~0.0004 kWh võrra ristata)
        least(sum(kwh) filter (where tag = 'grid_import_kw'),
              sum(kwh) filter (where tag = 'consumption_kw'))                            as grid_import_kwh
    from energia
    group by plokk_utc
)

select
    k.plokk_utc                                                 as ts_utc,
    k.plokk_utc at time zone 'Europe/Tallinn'                   as ts_eest,
    p.price_eur_kwh,
    k.consumption_kwh,
    k.grid_import_kwh,
    -- Tegelik arve: võrgust imporditud energia × spot-hind
    k.grid_import_kwh * p.price_eur_kwh                          as grid_cost_eur,
    -- Bruto kulu kui poleks päikest: kogu tarve × spot-hind
    k.consumption_kwh * p.price_eur_kwh                          as gross_cost_eur,
    -- Päikese sääst (välditud võrguost × hind)
    (k.consumption_kwh - k.grid_import_kwh) * p.price_eur_kwh    as solar_saving_eur
from kwh_wide k
join {{ ref('silver_electricity_prices') }} p
  on p.ts_utc = k.plokk_utc
order by k.plokk_utc
