with brand as (
    select
        distinct coalesce(brand, 'Unknown') as brand
    from
        {{source('Landing_ahussien','cars_data')}}
    )

select
    row_number() over () as brand_id,
    brand
from
    brand