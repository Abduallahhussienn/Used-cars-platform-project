with dim_model AS (
    SELECT distinct coalesce(model, 'Unknown') as model_name 
    FROM {{source('Landing_ahussien','cars_data')}}
)

select 
row_number() over() as model_id,
*
from
dim_model
