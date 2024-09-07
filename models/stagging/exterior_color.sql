with exterior_color as (
    select distinct 
    exterior_color
    from {{source('Landing_ahussien','cars_data')}}
)

select 
row_number() over() as exterior_color_id,
*
from exterior_color