with

    results as (select * from {{ source('formula1', 'results') }}),

    renamed as (
        select
            resultid as result_id,
            raceid as race_id,
            driverid as driver_id,
            constructorid as constructor_id,
            number as driver_number,
            grid,
            --position::int as position,
            iff(contains(position, '\N'), null, position) as driver_position,
            positiontext as position_text,
            positionorder as position_order,
            points,
            laps,
            iff(contains("TIME", '\N'), null, "TIME") as race_time,
            iff(contains(milliseconds, '\N'), null, milliseconds) as milliseconds,
            iff(contains(fastestlap, '\N'), null, fastestlap) as fastest_lap,
            "RANK" as driver_rank,
            iff(contains(fastestlaptime, '\N'),null,{{ convert_laptime("fastestlaptime") }}) as fastest_laptime,
            iff(contains(fastestlapspeed, '\N'), null, fastestlapspeed) as fastest_lapspeed,
            statusid as status_id
        from results
    )

select *
from renamed
