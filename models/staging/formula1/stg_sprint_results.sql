with sprint_results as (select * from {{ source('formula1', 'sprint_results') }}),

    renamed as (
        select
            result_id as result_id,
            race_id as race_id,
            driver_id as driver_id,
            constructor_id as constructor_id,
            number as driver_number,
            grid as grid,
            iff(contains("POSITION", '\N'), null, "POSITION") as driver_position,
            position_text as position_text,
            position_order as position_order,
            points as points,
            laps as laps,
            iff(contains("TIME", '\N'), null, "TIME") as sprint_time,
            iff(contains(milliseconds, '\N'), null, milliseconds) as milliseconds,
            iff(contains(fastest_lap, '\N'), null, fastest_lap) as fastest_lap,
            iff(
                contains(fastest_lap_time, '\N'),
                null,
                {{ convert_laptime("fastest_lap_time") }}
            ) as fastest_lap_time,
            status_id as status_id
        from sprint_results
    )

select *
from renamed
