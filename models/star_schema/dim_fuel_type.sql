with dim_fuel_type as (
    select distinct coalesce(fuel_type, 'Unknown') as fuel_type_name
    from {{source('Landing_ahussien','cars_data')}}
)

select 
row_number() over() as fuel_type_id,
*
from dim_fuel_type