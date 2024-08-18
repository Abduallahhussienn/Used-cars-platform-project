with junk_dim as (
    select distinct
     coalesce(Damaged,'Unknown') as Damaged,
     coalesce(first_owner,'Unknown') as first_owner,
     coalesce(personal_using,'Unknown') as personal_using,
     coalesce(turbo,'Unknown') as turbo,
     coalesce(alloy_wheels,'Unknown') as alloy_wheels
    from {{source('Landing_ahussien','cars_data')}}
)

select 
row_number() over() as junk_id,
*
from junk_dim