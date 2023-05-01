with sprint_results as (select * from {{ source('formula1', 'sprint_results') }}),

    renamed as (
        select
            resultid as result_id,
            raceid as race_id,
            driverid as driver_id,
            constructorid as constructor_id,
            number as driver_number,
            grid as grid,
            iff(contains("POSITION", '\N'), null, "POSITION") as driver_position,
            positiontext as position_text,
            positionorder as position_order,
            points as points,
            laps as laps,
            iff(contains("TIME", '\N'), null, "TIME") as sprint_time,
            iff(contains(milliseconds, '\N'), null, milliseconds) as milliseconds,
            iff(contains(fastestlap, '\N'), null, fastestlap) as fastest_lap,
            iff(
                contains(fastestlaptime, '\N'),
                null,
                {{ convert_laptime("fastestlaptime") }}
            ) as fastest_laptime,
            statusid as status_id
        from sprint_results
    )

select *
from renamed
