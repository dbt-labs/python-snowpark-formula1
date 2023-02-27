with

source  as (

    select * from {{ source('formula1','circuits') }}

), 

renamed as (
    select 
        circuitid as circuit_id,
        circuitref as circuit_ref,
        name as circuit_name,
        location,
        country,
        lat as latitude,
        lng as longitude,
        alt as altitude
        -- omit the url
    from source
)
select * from renamed