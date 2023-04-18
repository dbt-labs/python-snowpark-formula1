WITH constructors AS (
    SELECT * FROM {{ ref('stg_constructors') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['constructor_id']) }}      AS dim_constructor_id,
    constructor_id                                                  AS constructor_id,
    constructor_ref                                                 AS constructor_ref,
    constructor_name                                                AS constructor_name,
    constructor_nationality                                         AS constructor_nationality
    --constructor_url                         AS constructor_url
FROM constructors