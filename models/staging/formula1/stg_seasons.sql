with
    seasons as (select * from {{ source('formula1', 'seasons') }}),
    renamed as (
        select
            -- need surogate key with race_id, driver_id and stop
            "YEAR" as season_year, url as season_url
        from seasons
    )
select *
from renamed
