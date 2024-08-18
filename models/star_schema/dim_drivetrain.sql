with dim_drivetrain AS (
    SELECT distinct coalesce(drivetrain,'Unknown') as drivetrain_name 
    FROM {{source('Landing_ahussien','cars_data')}}
)

select 
row_number() over() as drivetrain_id,
*
from
dim_drivetrain