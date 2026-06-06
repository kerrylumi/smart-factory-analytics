-- OEE star-schema KPI-tabel: OEE = Availability × Performance × Quality.
-- Kõik kolm komponenti arvutatud masinate TOORSIGNAALIDEST (state + loendurid),
-- mitte simulaatori eelarvutatud `oee.*` tag'idest.
-- JOOKSEV KUMULATIIVNE minuti kaupa → sile, iga minut muutuv live-OEE trend.
-- Grain: masin × minut.

select
    a.machine,
    a.minut,
    a.availability,
    p.performance,
    q.quality,
    a.availability * p.performance * q.quality  as oee
from {{ ref('gold_oee_availability') }} a
join {{ ref('gold_oee_performance') }} p    on p.machine = a.machine and p.minut = a.minut
join {{ ref('gold_oee_quality') }} q        on q.machine = a.machine and q.minut = a.minut
