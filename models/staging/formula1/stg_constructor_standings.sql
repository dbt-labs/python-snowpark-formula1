with
    constructor_standings as (
        select * from {{ source('formula1', 'constructor_standings') }}
    ),
    renamed as (
        select
            constructorstandingsid as constructor_standings_id,
            raceid as race_id,
            constructorid as constructor_id,
            points as points,
            "POSITION" as constructor_position,
            positiontext as position_text,
            wins as wins
        from constructor_standings
    )
select *
from renamed
