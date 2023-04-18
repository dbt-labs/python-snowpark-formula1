WITH drivers AS (
    SELECT 
        driver_id               AS driver_id,
        driver_ref              AS driver_ref,
        driver_number           AS driver_number,
        driver_code             AS driver_code,
        forename                AS forename,
        surname                 AS surname,
        date_of_birth           AS date_of_birth,
        driver_nationality      AS driver_nationality
        --driver_url        AS driver_url
    FROM {{ ref('stg_drivers') }}
)

SELECT * FROM drivers