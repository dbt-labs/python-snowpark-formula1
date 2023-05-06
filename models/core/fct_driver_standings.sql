WITH driver_standings AS (
    SELECT 
        driver_standings_id     AS driver_standings_id,
        race_id                 AS race_id,
        driver_id               AS driver_id,
        driver_points           AS driver_points,
        driver_position         AS driver_position,
        position_text           AS position_text,
        driver_wins             AS driver_wins
    FROM {{ ref('stg_driver_standings') }}
)

SELECT * FROM driver_standings