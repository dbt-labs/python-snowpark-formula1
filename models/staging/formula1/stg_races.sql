with

source  as (

    select * from {{ source('formula1','races') }}

), 

renamed as (
    select 
        raceid as race_id,
        year as race_year, 
        round as race_round,
        circuitid as circuit_id,
        name as race_name,
        date as race_date,
        to_time(time) as race_time, 
        -- omit the url 
        fp1_date,
        fp1_time,
        fp2_date,
        fp2_time,
        fp3_date,
        fp3_time,
        quali_date,
        quali_time,
        sprint_date,
        sprint_time
    from source
)

select * from renamed 