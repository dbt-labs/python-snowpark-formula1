{{ config(
    tags=["race", "laptimes"],
    materialized = "incremental",
    unique_key = "result_id"
) }}

WITH results AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['result_id']) }}      AS fct_result_id,
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
        race_time           AS race_time,
        milliseconds        AS milliseconds,
        fastest_lap         AS fastest_lap,
        driver_rank         AS driver_rank,
        fastest_lap_time    AS fastest_lap_time,
        fastest_lap_speed   AS fastest_lap_speed,
        status_id           AS status_id
    FROM {{ ref('stg_results') }}
)

SELECT * FROM results