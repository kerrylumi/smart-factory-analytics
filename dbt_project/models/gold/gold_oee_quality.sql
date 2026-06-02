-- OEE komponent: Quality = (produced - scrap) / produced.
-- Toorsignaalid `parts_produced` ja `parts_scrap` on KUMULATIIVSED LOENDURID,
-- mis nullitakse iga töökäsu vahetusel. Seega ei tohi üle päeva lihtsalt max'i võtta:
-- tuvasta nullimised (value < lag), määra sessiooni-id kumulatiivse summaga,
-- võta iga sessiooni max ja summeeri sessioonid kokku.

with flagitud as (
    select
        machine,
        tag,
        ts_utc,
        value,
        case
            when value < lag(value) over (partition by machine, tag order by ts_utc)
            then 1 else 0
        end as reset_flag
    from {{ ref('silver_factory_telemetry') }}
    where tag in ('parts_produced', 'parts_scrap')
),

sessioonid as (
    select
        *,
        sum(reset_flag) over (partition by machine, tag order by ts_utc) as session_id
    from flagitud
),

sessiooni_max as (
    select
        machine,
        tag,
        date_trunc('day', max(ts_utc))  as paev,
        max(value)                      as cnt
    from sessioonid
    group by machine, tag, session_id
),

kokku as (
    select
        machine,
        paev,
        sum(cnt) filter (where tag = 'parts_produced')  as produced,
        sum(cnt) filter (where tag = 'parts_scrap')     as scrap
    from sessiooni_max
    group by machine, paev
)

select
    machine,
    paev,
    produced,
    scrap,
    (produced - scrap) / nullif(produced, 0)    as quality
from kokku
