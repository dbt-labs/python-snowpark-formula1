with

source  as (

    select * from {{ source('formula1','lap_times') }}

), 

renamed as (
    select 
        raceid as race_id, 
        driverid as driver_id,
        lap,
        position, 
        time as lap_time_formatted,
        milliseconds as lap_time_milliseconds
    from source
)

select * from renamed 