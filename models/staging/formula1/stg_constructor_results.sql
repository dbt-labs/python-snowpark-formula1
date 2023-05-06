with
    constructor_results as (
        select * from {{ source('formula1', 'constructor_results') }}
    ),
    renamed as (
        select
            constructor_results_id as constructor_results_id,
            race_id as race_id,
            constructor_id as constructor_id,
            points as constructor_points,
            iff(contains(status, '\N'), null, status) as status
        from constructor_results
    )
select *
from renamed
