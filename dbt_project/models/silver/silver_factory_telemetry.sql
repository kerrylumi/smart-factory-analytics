-- Tehase telemeetria puhastatud long-formaadis.
-- Transform: NULL-filter + Europe/Tallinn ajatempel. Pivot/agregeerimine toimub gold-kihis.

select
    dept,
    machine,
    tag,
    value,
    timestamp                               as ts_utc,
    timestamp at time zone 'Europe/Tallinn' as ts_eest
from {{ source('bronze', 'raw_factory_data') }}
where value is not null
