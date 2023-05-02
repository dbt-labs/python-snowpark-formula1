with

    driver_standings as (select * from {{ source('formula1', 'driver_standings') }}),

    renamed as (
        select
            driver_standings_id as driver_standings_id,
            race_id as race_id,
            driver_id as driver_id,
            points as driver_points,
            "POSITION" as driver_position,
            position_text as position_text,
            wins as driver_wins
        from driver_standings
    )

select *
from renamed
