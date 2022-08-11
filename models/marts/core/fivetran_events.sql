with traffic_events as (
    select * from {{source ('traffic_weather_prod.traffic', 'traffic')}}
),

weather_events as (
    select city as city_2, type as weather_event, start_time_utc_ as weather_start, end_time_utc_ as weather_end  from {{source ('traffic_weather_prod.weather', 'weather')}}
),

final as(
select *
    from traffic_events
    left join weather_events
    on traffic_events.city=weather_events.city_2 and (weather_events.weather_start <= traffic_events.start_time_utc_ and weather_events.weather_end >= traffic_events.start_time_utc_ )
)

select * from final