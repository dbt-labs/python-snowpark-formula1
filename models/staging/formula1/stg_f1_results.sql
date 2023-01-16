with

source  as (

    select * from {{ source('formula1','results') }}

), 

renamed as (
    select 
        resultid as result_id,
        raceid as race_id,
        driverid as driver_id,
        constructorid as constructor_id,
        number as driver_number,
        grid, 
        position::int as position,
        positiontext as position_text,
        positionorder as position_order,
        points,
        laps,
        time as results_time_formatted, 
        milliseconds as results_milliseconds,
        fastestlap as fastest_lap,
        rank as results_rank,
        fastestlaptime as fastest_lap_time_formatted,
        fastestlapspeed::decimal(6,3) as fastest_lap_speed, 
        statusid as status_id
    from source
)

select * from renamed 