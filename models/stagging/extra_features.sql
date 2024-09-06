with extra_features as (
    select distinct
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
        {{source('Landing_ahussien','cars_data')}}    
)

select 
    ROW_NUMBER() OVER () as extra_features_id,
    *
from
    extra_features