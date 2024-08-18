select
row_number() over() as car_id
,brand_id
,model_id
,year
,coalesce(mileage,"0") as mileage
,engine_id
,transmission_id
,fuel_type_id
,drivetrain_id
,coalesce(min_mpg,"0") as min_mpg
,coalesce(max_mpg,"0") as max_mpg
,junk_id
,extra_features_id
,interior_color_id
,exterior_color_id
,case 
when price is null then "0"
when price = 'ot Priced' then "0"
else price
end as price
from
{{source("Landing_ahussien","cars_data")}} c
left join
{{source("dbt_ahussien","dim_brand")}} b 
on c.brand = b.brand_name
left join
{{source("dbt_ahussien","dim_model")}} m
on c.model = m.model_name
left join
{{source("dbt_ahussien","dim_engine")}} e
on c.engine = e.engine_name and coalesce(c.engine_size,"Unknown") = e.engine_size
left join
{{source("dbt_ahussien","dim_transmission")}} t
on coalesce(c.transmission,"Unknown") = t.transmission_name and coalesce(c.automatic_transmission,"Unknown") = t.automatic_transmission
left join
{{source("dbt_ahussien","dim_fuel_type")}} f
on coalesce(c.fuel_type,"Unknown") = f.fuel_type_name
left join
{{source("dbt_ahussien","dim_drivetrain")}} d
on coalesce(c.drivetrain,"Unknown") = d.drivetrain_name
left join
{{source("dbt_ahussien","junk_dim")}} j
on coalesce(c.damaged,"Unknown") = j.Damaged
and coalesce(c.first_owner,"Unknown") = j.first_owner
and coalesce(c.personal_using,"Unknown") = j.personal_using
and coalesce(c.turbo,"Unknown") = j.turbo
and coalesce(c.alloy_wheels,"Unknown") = j.alloy_wheels
left join
{{source("dbt_ahussien","dim_extra_features")}} ef
on coalesce(c.adaptive_cruise_control,"Unknown") = ef.adaptive_cruise_control
and coalesce(c.navigation_system,"Unknown") = ef.navigation_system
and coalesce(c.power_liftgate,"Unknown") = ef.power_liftgate
and coalesce(c.backup_camera,"Unknown") = ef.backup_camera
and coalesce(c.keyless_start,"Unknown") = ef.keyless_start
and coalesce(c.remote_start,"Unknown") = ef.remote_start
and coalesce(c.sunroof_or_moonroof,"Unknown") = ef.sunroof_moonroof
and coalesce(c.automatic_emergency_braking,"Unknown") = ef.automatic_emergency_braking
and coalesce(c.stability_control,"Unknown") = ef.stability_control
and coalesce(c.leather_seats,"Unknown") = ef.leather_seats
and coalesce(c.memory_seat,"Unknown") = ef.memory_seat
and coalesce(c.third_row_seating,"Unknown") = ef.third_row_seating
and coalesce(c.apple_car_play_or_android_auto,"Unknown") = ef.apple_carPlay_android_auto
and coalesce(c.bluetooth,"Unknown") = ef.bluetooth
and coalesce(c.usb_Port,"Unknown") = ef.usb_Port
and coalesce(c.heated_seats,"Unknown") = ef.heated_seats
left join 
{{source("dbt_ahussien","dim_interior_color")}} ic
on coalesce(c.interior_color,"Unknown") = ic.interior_color
left join 
{{source("dbt_ahussien","dim_exterior_color")}} xc
on coalesce(c.exterior_color,"Unknown") = xc.exterior_color