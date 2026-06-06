-- OEE komponent: Quality = (produced - scrap) / produced, JOOKSEV KUMULATIIVNE minuti kaupa.
-- `parts_produced`/`parts_scrap` on kumulatiivsed loendurid → minuti juurdekasv = delta
-- (reset = negatiivne delta → 0). Siis kumulatiivne summa algusest (running window).
-- Hõreda andmega (paljud minutid 0 tükki) püsib kvaliteet määratud, sest kasutame
-- jooksvaid kogusummasid; enne esimest tükki quality = 1.0. Grain: masin × minut.

with samples as (
    select
        machine,
        tag,
        ts_utc,
        value - lag(value) over (partition by machine, tag order by ts_utc) as delta
    from {{ ref('silver_factory_telemetry') }}
    where tag in ('parts_produced', 'parts_scrap')
),

per_min as (
    select
        machine,
        date_trunc('minute', ts_utc)                                            as minut,
        sum(case when tag = 'parts_produced' and delta > 0 then delta else 0 end) as produced,
        sum(case when tag = 'parts_scrap'    and delta > 0 then delta else 0 end) as scrap
    from samples
    group by machine, date_trunc('minute', ts_utc)
),

cum as (
    select
        machine,
        minut,
        sum(produced) over (partition by machine order by minut rows unbounded preceding) as produced_cum,
        sum(scrap)    over (partition by machine order by minut rows unbounded preceding) as scrap_cum
    from per_min
)

select
    machine,
    minut,
    produced_cum,
    scrap_cum,
    case
        when produced_cum > 0 then (produced_cum - scrap_cum) / produced_cum
        else 1.0
    end as quality
from cum
