WITH model AS (
    SELECT DISTINCT
        model,
        brand,
        drivetrain
    
    FROM
        {{source('Landing_ahussien','cars_data')}}
    

),
brand AS (
    SELECT
        brand_id,
        brand.brand as brand
    FROM
        {{source('dbt_ahussien_Stagging_ahussien','brand')}} 
)

SELECT
    ROW_NUMBER() OVER () AS model_id,
    m.model,
    drivetrain,
    brand_id
FROM
    model m
LEFT JOIN
    brand b
ON
    m.brand = b.brand.brand