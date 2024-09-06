WITH fuel_type AS (
    SELECT DISTINCT
        fuel_type
    FROM
        {{source('Landing_ahussien','cars_data')}}
    
)

SELECT
    ROW_NUMBER() OVER () AS fuel_type_id,
    *
    
FROM
    fuel_type