with

races  as (

    select * from {{ source('formula1','races') }}

), 

renamed as (
    select 
        race_id as race_id,
        "YEAR" as race_year, 
        "ROUND" as race_round,
        circuit_id as circuit_id,
        name as race_name,
        "DATE" as race_date,
        "TIME" as race_time, 
        url as race_url,
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
    from races
)

select * from renamed