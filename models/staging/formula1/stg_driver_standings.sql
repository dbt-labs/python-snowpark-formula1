with

    driver_standings as (select * from {{ source('formula1', 'driver_standings') }}),

    renamed as (
        select
            driverstandingsid as driver_standings_id,
            raceid as race_id,
            driverid as driver_id,
            points as driver_points,
            "POSITION" as driver_position,
            positiontext as position_text,
            wins as driver_wins
        from driver_standings
    )

select *
from renamed
