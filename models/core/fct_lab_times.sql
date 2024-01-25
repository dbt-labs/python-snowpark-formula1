with lap_times as (
    select 
        {{ dbt_utils.generate_surrogate_key(['race_id', 'driver_id', 'lap']) }} as lap_times_id,
        race_id                                                                 as race_id,
        driver_id                                                               as driver_id,
        lap                                                                     as lap,
        driver_position                                                         as driver_position,
        lap_time_formatted                                                      as lap_time_formatted,
        official_laptime                                                        as official_laptime,
        lap_time_milliseconds                                                   as lap_time_milliseconds
    from {{ ref('stg_lap_times') }}
)
select * from lap_times