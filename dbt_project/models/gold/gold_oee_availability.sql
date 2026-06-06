-- OEE komponent: Availability = run_time / planned_time, JOOKSEV KUMULATIIVNE minuti kaupa.
-- Toorsignaal `state` (PackML, EXECUTE = 3), aja-kaalutud (LEAD) kestus minuti kaupa,
-- siis kumulatiivne summa algusest (running window) → sile, alati määratud.
-- Väljastab ka run_s_cum (performance taaskasutab). Grain: masin × minut.

with stream as (
    select
        machine,
        ts_utc,
        value                                                       as state_code,
        lead(ts_utc) over (partition by machine order by ts_utc)    as next_ts
    from {{ ref('silver_factory_telemetry') }}
    where tag = 'state'
),

per_min as (
    select
        machine,
        date_trunc('minute', ts_utc)                                              as minut,
        sum(extract(epoch from (next_ts - ts_utc)))                                          as planned_s,
        coalesce(sum(extract(epoch from (next_ts - ts_utc))) filter (where state_code = 3), 0) as run_s
    from stream
    where next_ts is not null
      -- ignoreeri andmeauke: >120s intervall pole tegelik olek, vaid puuduv andmestik
      and extract(epoch from (next_ts - ts_utc)) <= 120
    group by machine, date_trunc('minute', ts_utc)
),

cum as (
    select
        machine,
        minut,
        sum(run_s)     over (partition by machine order by minut rows unbounded preceding) as run_s_cum,
        sum(planned_s) over (partition by machine order by minut rows unbounded preceding) as planned_s_cum
    from per_min
)

select
    machine,
    minut,
    run_s_cum,
    planned_s_cum,
    run_s_cum / nullif(planned_s_cum, 0) as availability
from cum
