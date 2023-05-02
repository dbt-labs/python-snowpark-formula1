WITH status AS (

    SELECT * FROM {{ source('formula1','status') }}

), 

renamed AS (
    SELECT 
        statusid AS status_id,
        status 
    FROM status
)

SELECT * FROM renamed 