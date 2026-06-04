-- Seisakute (downtime) jaotus PackML state-koodide kaupa.
-- Aja-kaalutud kestus iga oleku kohta (sama LEAD-muster nagu gold_oee_availability),
-- state_code → inimloetav silt. Downtime = mitte-EXECUTE olekud (state_code <> 3).
-- Põhjuse-sildid ("Material shortage" jne) pole andmetes (string-tag'id NULL),
-- seega kasutame PackML olekuid. Grain: masin × päev × olek.

with stream as (
    select
        machine,
        ts_utc,
        value                                                       as state_code,
        lead(ts_utc) over (partition by machine order by ts_utc)    as next_ts
    from {{ ref('silver_factory_telemetry') }}
    where tag = 'state'
)

select
    machine,
    date_trunc('day', ts_utc)   as paev,
    state_code,
    case state_code
        when 0 then 'STOPPED'    when 1 then 'STARTING'   when 2 then 'IDLE'
        when 3 then 'EXECUTE'    when 4 then 'COMPLETING' when 5 then 'HELD'
        when 6 then 'SUSPENDED'  when 7 then 'ABORTED'    else 'UNKNOWN'
    end                         as state_nimi,
    sum(extract(epoch from (next_ts - ts_utc)))         as kestus_s,
    sum(extract(epoch from (next_ts - ts_utc))) / 60.0  as kestus_min
from stream
where next_ts is not null
group by machine, date_trunc('day', ts_utc), state_code
