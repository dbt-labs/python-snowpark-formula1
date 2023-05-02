with
    constructor_results as (
        select * from {{ source('formula1', 'constructor_results') }}
    ),
    renamed as (
        select
            constructorresultsid as constructor_results_id,
            raceid as race_id,
            constructorid as constructor_id,
            points as constructor_points,
            iff(contains(status, '\N'), null, status) as status
        from constructor_results
    )
select *
from renamed
