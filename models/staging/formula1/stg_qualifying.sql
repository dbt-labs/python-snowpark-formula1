with
    qualifying as (select * from {{ source("formula1", "qualifying") }}),

    renamed as (
        select
            qualifying_id as qualifying_id,
            race_id as race_id,
            driver_id as driver_id,
            constructor_id as constructor_id,
            number as driver_number,
            "POSITION" as qualifying_position,
            iff(contains(q1, '\N'), null, {{ convert_laptime("q1") }}) as q1_time,
            iff(contains(q2, '\N'), null, {{ convert_laptime("q2") }}) as q2_time,
            iff(contains(q3, '\N'), null, {{ convert_laptime("q3") }}) as q3_time
        from qualifying
    )

select *
from renamed
