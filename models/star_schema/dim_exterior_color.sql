with dim_exterior_color as (
    select distinct coalesce(exterior_color,'Unknown') as exterior_color
    from {{source('Landing_ahussien','cars_data')}}
)

select 
row_number() over() as exterior_color_id,
*
from dim_exterior_color