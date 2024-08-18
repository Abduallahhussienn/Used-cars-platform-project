with dim_brand AS (
    SELECT distinct coalesce(brand, 'Unknown') as brand_name
    FROM {{source('Landing_ahussien','cars_data')}}
)

select 
row_number() over() as brand_id,
*
from
dim_brand