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
        name as circuit_name,
        date as race_date,
        to_time(time) as race_time, 
        -- omit the url 
        fp1_date as free_practice_1_date,
        fp1_time as free_practice_1_time,
        fp2_date as free_practice_2_date,
        fp2_time as free_practice_2_time,
        fp3_date as free_practice_3_date,
        fp3_time as free_practice_3_time,
        quali_date as qualifying_date,
        quali_time as qualifying_time,
        sprint_date,
        sprint_time
    from source
)

select * from renamed 