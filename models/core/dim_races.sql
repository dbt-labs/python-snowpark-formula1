WITH races AS (
    SELECT 
        race_id         AS race_id,
        race_year       AS race_year,
        race_round      AS race_round,
        circuit_id      AS circuit_id,
        race_name       AS race_name,
        race_date       AS race_date,
        race_time       AS race_time,
        --race_url        AS race_url,
        fp1_date        AS fp1_date,
        fp1_time        AS fp1_time,
        fp2_date        AS fp2_date,
        fp2_time        AS fp2_time,
        fp3_date        AS fp3_date,
        fp3_time        AS fp3_time,
        quali_date      AS quali_date,
        quali_time      AS quali_time,
        sprint_date     AS sprint_date,
        sprint_time     AS sprint_time
    FROM {{ ref('stg_races') }}
)

SELECT * FROM races