-- Energiakulu ÜHE valmistatud tüki kohta (Specific Energy Consumption, SEC).
-- Ühendab tehase energiatarbe (gold_energy: kWh + Eleringi spot-hind 15-min plokis)
-- ja kõigi masinate toodetud tükkide juurdekasvu samas plokis.
--
-- NB: energiat mõõdetakse ainult tehase tasandil (`main` meeter), seega intensiivsus
-- on TEHASE-TASANDI: kogu tarve / kõik toodetud tükid. parts_produced on kumulatiivne
-- loendur → tükke plokis = juurdekasv (delta), reset (negatiivne) loeb nulliks.
-- Grain: 15-min plokk → dünaamika ajas.

with parts_delta as (
    select
        ts_utc,
        value - lag(value) over (partition by machine order by ts_utc) as delta
    from {{ ref('silver_factory_telemetry') }}
    where tag = 'parts_produced'
),

parts_plokk as (
    select
        date_trunc('hour', ts_utc)
            + floor(extract(minute from ts_utc)::int / 15) * interval '15 minutes' as plokk_utc,
        sum(case when delta > 0 then delta else 0 end) as parts
    from parts_delta
    group by 1
)

select
    e.ts_utc,
    e.ts_eest,
    e.price_eur_kwh,
    e.consumption_kwh,
    p.parts,
    -- füüsiline energiaintensiivsus
    e.consumption_kwh / nullif(p.parts, 0)                      as kwh_per_part,
    -- rahaline: tüki energia hinnatud selle ploki spot-hinnaga
    e.consumption_kwh * e.price_eur_kwh / nullif(p.parts, 0)    as cost_per_part_eur
from {{ ref('gold_energy') }} e
join parts_plokk p on p.plokk_utc = e.ts_utc
where p.parts > 0
order by e.ts_utc
