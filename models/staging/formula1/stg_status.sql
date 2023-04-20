WITH source AS (

    SELECT * FROM {{ source('formula1','status') }}

), 

renamed AS (
    SELECT 
        statusid AS status_id,
        status 
    FROM source
)

SELECT * FROM renamed 