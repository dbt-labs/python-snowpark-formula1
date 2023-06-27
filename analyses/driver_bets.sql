with predictions as (
    select * from {{ ref('apply_prediction_to_position') }}
),

encoding_mapping as (
    select * from {{ ref('encoding_mapping') }}
)

select 
    original_driver_value, 
    encoded_driver_value, 
    predictions.driver,
    AVG(position_predicted) as avg_position_predicted
from predictions
join encoding_mapping on predictions.driver=encoding_mapping.encoded_driver_value
group by 1,2,3
