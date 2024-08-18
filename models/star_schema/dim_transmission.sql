with dim_transmission AS (
    SELECT distinct coalesce(transmission, 'Unknown') as transmission_name,
    coalesce(automatic_transmission,'Unknown') as automatic_transmission
    FROM {{source('Landing_ahussien','cars_data')}}
)

select 
row_number() over() as transmission_id,
*
from
dim_transmission
