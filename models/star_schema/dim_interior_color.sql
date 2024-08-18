with dim_interior_color as (
    select distinct coalesce(interior_color,'Unknown') as interior_color
    from {{source('Landing_ahussien','cars_data')}}
)

select 
row_number() over() as interior_color_id,
*
from dim_interior_color