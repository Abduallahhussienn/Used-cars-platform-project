with interior_color as (
    select distinct 
    interior_color
    from {{source('Landing_ahussien','cars_data')}}
)

select 
row_number() over() as interior_color_id,
*
from interior_color