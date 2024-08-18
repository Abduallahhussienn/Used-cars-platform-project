with dim_extra_features as (
    select distinct
     coalesce(adaptive_cruise_control,'Unknown') as adaptive_cruise_control,
     coalesce(navigation_system,'Unknown') as navigation_system,
     coalesce(power_liftgate,'Unknown') as power_liftgate,
     coalesce(backup_camera,'Unknown') as backup_camera,
     coalesce(keyless_start,'Unknown') as keyless_start,
     coalesce(remote_start,'Unknown') as remote_start,
     coalesce(sunroof_or_moonroof,'Unknown') as sunroof_moonroof,
     coalesce(automatic_emergency_braking,'Unknown') as automatic_emergency_braking,
     coalesce(stability_control,'Unknown') as stability_control,
     coalesce(leather_seats,'Unknown') as leather_seats,
     coalesce(memory_seat,'Unknown') as memory_seat,
     coalesce(third_row_seating,'Unknown') as third_row_seating,
     coalesce(apple_car_play_or_android_auto,'Unknown') as apple_carPlay_android_auto,
     coalesce(bluetooth,'Unknown') as bluetooth,
     coalesce(usb_Port,'Unknown') as usb_Port,
     coalesce(heated_seats,'Unknown') as heated_seats
    from {{source('Landing_ahussien','cars_data')}}
)

select 
row_number() over() as extra_features_id,
*
from dim_extra_features