WITH pitstops AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['race_id', 'driver_id', 'stop_number']) }}         AS pitstop_id,
        race_id                                                                                 AS race_id,
        driver_id                                                                               AS driver_id,
        stop_number                                                                             AS stop_number,
        lap                                                                                     AS lap,
        pit_stop_time                                                                           AS pitstop_time,
        pit_stop_duration_seconds                                                               AS pit_stop_duration_seconds,
        pit_stop_duration                                                                       AS pit_stop_duration,
        pit_stop_milliseconds                                                                   AS pit_stop_milliseconds
    FROM {{ ref('stg_pit_stops') }}
)

SELECT * FROM pitstops