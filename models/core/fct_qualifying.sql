WITH qualifying AS (
    SELECT 
        qualifying_id                               AS qualifying_id,
        race_id                                     AS race_id,
        driver_id                                   AS driver_id,
        constructor_id                              AS constructor_id,
        driver_number                               AS driver_number,
        qualifying_position                         AS qualifying_position,
        q1_time                                     AS q1_time,
        q2_time                                     AS q2_time,
        q3_time                                     AS q3_time
    FROM {{ ref('stg_qualifying') }}
)

SELECT * FROM qualifying