-- Eesti elektrihind UTC ja Europe/Tallinn ajatempelistega.
-- Transform: ajavöönd UTC -> EEST, EUR/MWh -> EUR/kWh, NULL filter.

select
    country,
    ts_utc,
    ts_utc at time zone 'Europe/Tallinn'    as ts_eest,
    price_eur_mwh,
    price_eur_mwh / 1000.0                  as price_eur_kwh,
    loaded_at
from {{ source('bronze', 'br_electricity_prices') }}
where country = 'ee'
  and price_eur_mwh is not null
