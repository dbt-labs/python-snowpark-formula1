with stg_f1__pit_stops as 
(
    select * from {{ ref('stg_f1_pit_stops') }}
),

pit_stops_per_race as (
    select 
        race_id,
        driver_id,
        stop_number,
        lap,
        lap_time_formatted,
        pit_stop_duration_seconds,
        pit_stop_milliseconds, 
        max(stop_number) over (partition by race_id,driver_id) as total_pit_stops_per_race
    from stg_f1__pit_stops
 
)

select * from pit_stops_per_race