with dim_engine AS (
    SELECT distinct coalesce(engine, 'Unknown') as engine_name,
    coalesce(engine_size,'Unknown') as engine_size
    FROM {{source('Landing_ahussien','cars_data')}}
)

select 
row_number() over() as engine_id,
*
from
dim_engine
