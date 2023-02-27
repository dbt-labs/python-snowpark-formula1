{{
    config(
        enabled=true,
        severity='error',
        tags = ['bi']
    )
}}

with lap_times_moving_avg as ( select * from {{ ref('lap_times_moving_avg') }} )

select *
from   lap_times_moving_avg 
where  lap_moving_avg_5_years < 0 and lap_moving_avg_5_years is not null