{{ config(
    tags=["dimension", "track"]
) }}

WITH circuits AS (
    SELECT * FROM {{ ref('stg_circuits') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['circuit_id']) }}      AS dim_circuit_id,
    circuit_id                                                  AS circuit_id,
    circuit_ref                                                 AS circuit_ref,
    circuit_name                                                AS circuit_name,
    location                                                    AS location,
    country                                                     AS country,                
    latitude                                                    AS latitude,
    longitude                                                   AS longitude,
    altitude                                                    AS altitude
    --circuit_url                                                 AS circuit_url
FROM circuits