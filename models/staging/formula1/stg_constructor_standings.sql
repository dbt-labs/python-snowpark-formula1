with
    constructor_standings as (
        select * from {{ source('formula1', 'constructor_standings') }}
    ),
    renamed as (
        select
            constructor_standings_id as constructor_standings_id,
            race_id as race_id,
            constructor_id as constructor_id,
            points as points,
            "POSITION" as constructor_position,
            position_text as position_text,
            wins as wins
        from constructor_standings
    )
select *
from renamed
