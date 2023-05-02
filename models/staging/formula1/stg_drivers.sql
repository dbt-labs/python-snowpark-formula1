with

    drivers as (select * from {{ source('formula1', 'drivers') }}),

    renamed as (
        select
            driver_id as driver_id,
            driver_ref as driver_ref,
            number as driver_number,
            code as driver_code,
            forename,
            surname,
            dob as date_of_birth,
            nationality as driver_nationality, 
            url as driver_url
        from drivers
    )

select *
from renamed
