WITH lap_times AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['race_id', 'driver_id', 'lap']) }}         AS lap_times_id,
        race_id                                                                         AS race_id,
        driver_id                                                                       AS driver_id,
        lap                                                                             AS lap,
        driver_position                                                                 AS driver_position,
        lap_time_formatted                                                              AS lap_time_formatted,
        official_laptime                                                                AS official_laptime,
        lap_time_milliseconds                                                           AS lap_time_milliseconds
    FROM {{ ref('stg_lap_times') }}
)

SELECT * FROM lap_times