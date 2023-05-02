with

    pit_stops as (select * from {{ source('formula1', 'pit_stops') }}),

    renamed as (
        select
            raceid as race_id,
            driverid as driver_id,
            stop as stop_number,
            lap,
            "TIME" as pit_stop_time,
            duration as pit_stop_duration_seconds,
            {{ convert_laptime("pit_stop_duration_seconds") }} as pit_stop_duration,
            milliseconds as pit_stop_milliseconds
        from pit_stops
    )

select
    {{ dbt_utils.generate_surrogate_key(["race_id", "driver_id", "stop_number"]) }}
    as pitstop_id,
    *
from renamed
