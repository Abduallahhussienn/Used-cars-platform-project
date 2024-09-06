with engine as (
    select distinct
        engine ,
        engine_size,
        fuel_type
    from
        {{source('Landing_ahussien','cars_data')}}
    

),
fuel_type AS (
    SELECT
        fuel_type_id,
        fuel_type
    FROM
        {{ source('dbt_ahussien_Stagging_ahussien', 'fuel_type') }}  


)
SELECT
    ROW_NUMBER() OVER () AS engine_id,
    e.engine,
    e.engine_size,
    f.fuel_type_id
FROM
    engine e
LEFT JOIN
    fuel_type f
ON
    e.fuel_type = f.fuel_type.fuel_type