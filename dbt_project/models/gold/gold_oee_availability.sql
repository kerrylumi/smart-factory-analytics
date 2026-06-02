-- OEE komponent: Availability = run_time / planned_time.
-- Arvutus toorsignaalist `state` (PackML kood, EXECUTE = 3) AJA-KAALUTULT:
-- iga oleku kestus = järgmise näidu aeg miinus praegune (LEAD window).
-- NB: NÄIDETE LUGEMINE (count(state=3)/count(*)) oleks vale — näidud pole ühtlaselt jaotunud.

with stream as (
    select
        machine,
        ts_utc,
        value                                                       as state_code,
        lead(ts_utc) over (partition by machine order by ts_utc)    as next_ts
    from {{ ref('silver_factory_telemetry') }}
    where tag = 'state'
),

kestused as (
    select
        machine,
        date_trunc('day', ts_utc)                       as paev,
        extract(epoch from (next_ts - ts_utc))          as kestus_s,
        state_code
    from stream
    where next_ts is not null
)

select
    machine,
    paev,
    sum(kestus_s)                                       as planned_s,
    sum(kestus_s) filter (where state_code = 3)         as run_s,
    sum(kestus_s) filter (where state_code = 3)
        / nullif(sum(kestus_s), 0)                      as availability
from kestused
group by machine, paev
