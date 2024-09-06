with car as (
    select
        model,
        price,
        damaged,
        first_owner,
        personal_using,
        mileage,
        turbo,
        min_mpg,
        max_mpg,
        year,
        interior_color,
        exterior_color,
        drivetrain,
        transmission,
        engine,
        alloy_wheels,
        adaptive_cruise_control,
        navigation_system,
        power_liftgate,
        backup_camera,
        keyless_start,
        remote_start,
        sunroof_or_moonroof,
        automatic_emergency_braking,
        stability_control,
        leather_seats,
        memory_seat,
        third_row_seating,
        apple_car_play_or_android_auto,
        bluetooth,
        usb_port,
        heated_seats
    from
        {{ source('Landing_ahussien', 'cars_data') }}

),
model AS (
    SELECT
        model_id,
        model,
        drivetrain
    FROM
        {{ source('dbt_ahussien_Stagging_ahussien', 'model') }}
),
transmission AS (
    SELECT
        transmission_id,
        transmission
    FROM
        {{ source('dbt_ahussien_Stagging_ahussien', 'transmission') }}
),
engine AS (
    SELECT
        engine_id,
        engine 
    FROM
        {{ source('dbt_ahussien_Stagging_ahussien', 'engine') }}
),
interior_color AS (
    select
    interior_color_id,
    interior_color
    FROM
        {{ source('dbt_ahussien_Stagging_ahussien', 'interior_color') }}
),
exterior_color AS (
    select 
    exterior_color_id,
    exterior_color
    FROM
        {{ source('dbt_ahussien_Stagging_ahussien', 'exterior_color') }}
),
extra_features AS (
    select
    extra_features_id,
    alloy_wheels,
    adaptive_cruise_control,
    navigation_system,
    power_liftgate,
    backup_camera,
    keyless_start,
    remote_start,
    sunroof_or_moonroof,
    automatic_emergency_braking,
    stability_control,
    leather_seats,
    memory_seat,
    third_row_seating,
    apple_car_play_or_android_auto,
    bluetooth,
    usb_port,
    heated_seats
    FROM
        {{ source('dbt_ahussien_Stagging_ahussien', 'extra_features') }}
)
SELECT
    row_number() over() as car_id,
    c.price,
    c.damaged,
    c.first_owner,
    c.personal_using,
    c.mileage,
    c.turbo,
    c.min_mpg,
    c.max_mpg,
    c.year,
    ic.interior_color_id,
    xc.exterior_color_id,
    m.model_id,
    t.transmission_id,
    e.engine_id,
    ef.extra_features_id
FROM
    car c
LEFT JOIN
    model m
ON
    c.model = m.model.model and  c.drivetrain = m.drivetrain
LEFT JOIN
     transmission t
 ON
     c.transmission = t.transmission.transmission
LEFT JOIN
    engine e
ON
    c.engine = e.engine.engine
left join
extra_features ef
on c.adaptive_cruise_control = ef.adaptive_cruise_control
and c.navigation_system = ef.navigation_system
and c.power_liftgate = ef.power_liftgate
and c.backup_camera = ef.backup_camera
and c.keyless_start = ef.keyless_start
and c.remote_start = ef.remote_start
and c.sunroof_or_moonroof = ef.sunroof_or_moonroof
and c.automatic_emergency_braking = ef.automatic_emergency_braking
and c.stability_control = ef.stability_control
and c.leather_seats = ef.leather_seats
and c.memory_seat = ef.memory_seat
and c.third_row_seating = ef.third_row_seating
and c.apple_car_play_or_android_auto = ef.apple_car_play_or_android_auto
and c.bluetooth = ef.bluetooth
and c.usb_Port = ef.usb_Port
and c.heated_seats = ef.heated_seats
left join 
interior_color ic
on c.interior_color = ic.interior_color.interior_color
left join 
exterior_color xc
on c.exterior_color = xc.exterior_color.exterior_color
