WITH constructor_results AS (
    SELECT * FROM {{ ref('stg_constructor_results') }}
)

SELECT 
    constructor_results_id,
    race_id,
    constructor_id,
    constructor_points,
    status
FROM constructor_results