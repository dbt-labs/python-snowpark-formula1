WITH sprint_results AS (
    SELECT 
        result_id           AS result_id,
        race_id             AS race_id,
        driver_id           AS driver_id,
        constructor_id      AS constructor_id,
        driver_number       AS driver_number,
        grid                AS grid,
        driver_position     AS driver_position,
        position_text       AS position_text,
        position_order      AS position_order,
        points              AS points,
        laps                AS laps,
        sprint_time         AS sprint_time,
        milliseconds        AS milliseconds,
        fastest_lap         AS fastest_lap,
        fastest_lap_time     AS fastest_lap_time,
        status_id           AS status_id
    FROM {{ ref('stg_sprint_results') }}
)

SELECT * FROM sprint_results