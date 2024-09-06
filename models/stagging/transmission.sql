with transmission as (
    select distinct
        transmission,
        automatic_transmission
    from
        {{source('Landing_ahussien','cars_data')}}
    
        
)

select
    row_number() over () as transmission_id,
    *
from
    transmission