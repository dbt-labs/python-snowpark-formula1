WITH status AS (
    SELECT 
        status_id   AS status_id,
        status      AS status
    FROM {{ ref('stg_status') }}
)

SELECT * FROM status