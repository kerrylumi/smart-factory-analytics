-- OEE komponent: Performance = tegelik kiirus / ideaalne kiirus, JOOKSEV KUMULATIIVNE.
-- = kumulatiivsed toodetud osad / (kumulatiivne run_h × ideaalmäär parts/hour).
-- run_s_cum tuleb availability-mudelist, produced_cum quality-mudelist,
-- ideaalmäär seemnest `ideal_cycle_rates`. Enne mis tahes run-aega performance = 0.
-- Grain: masin × minut.

with tyybid as (
    select
        machine,
        case
            when machine like 'laser%'       then 'laser_cutter'
            when machine like 'press_brake%' then 'press_brake'
            when machine like 'coating%'     then 'powder_coating_line'
            when machine like 'assembly%'    then 'assembly'
            when machine like 'agv%'         then 'agv'
        end as machine_type
    from (select distinct machine from {{ ref('silver_factory_telemetry') }}) m
)

select
    a.machine,
    a.minut,
    coalesce(
        least(1.0, q.produced_cum / nullif(a.run_s_cum / 3600.0 * r.parts_per_hour, 0)),
        0
    ) as performance
from {{ ref('gold_oee_availability') }} a
join tyybid t                            on t.machine = a.machine
join {{ ref('ideal_cycle_rates') }} r    on r.machine_type = t.machine_type
join {{ ref('gold_oee_quality') }} q     on q.machine = a.machine and q.minut = a.minut
