with

source  as (

    select * from {{ source('formula1','pit_stops') }}

), 

renamed as (
    select 
        raceid as race_id,
        driverid as driver_id,
        stop as stop_number,
        lap, 
        time as pit_stop_time,
        duration as pit_stop_duration_seconds, 
        {{ convert_laptime('pit_stop_duration_seconds') }} as pit_stop_duration,
        milliseconds as pit_stop_milliseconds
    from source
)

select * from renamed 
