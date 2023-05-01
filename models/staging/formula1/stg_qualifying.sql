with
    qualifying as (select * from {{ source("formula1", "qualifying") }}),

    renamed as (
        select
            qualifyid as qualifying_id,
            raceid as race_id,
            driverid as driver_id,
            constructorid as constructor_id,
            number as driver_number,
            "POSITION" as qualifying_position,
            iff(contains(q1, '\N'), null, {{ convert_laptime("q1") }}) as q1_time,
            iff(contains(q2, '\N'), null, {{ convert_laptime("q2") }}) as q2_time,
            iff(contains(q3, '\N'), null, {{ convert_laptime("q3") }}) as q3_time
        from qualifying
    )

select *
from renamed
