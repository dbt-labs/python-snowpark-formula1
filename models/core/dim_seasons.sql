WITH seasons AS (
    SELECT 
        season_year     AS season_year,
        season_url      AS season_url
    FROM {{ ref('stg_seasons') }}
)

SELECT * FROM seasons