{{
    config(
      re_data_monitored=true,
      re_data_time_filter='day'
    )
}}
-- select cities with number of daily traffic and weather events
with daily_traffic_events as (
    select * from {{ref('stg_daily_traffic_events_with_severity')}}
) ,
    
daily_weather_events as (
    select * from {{ref('stg_daily_weather_events_with_severity')}}
),

final as(
select 
    daily_traffic_events.city as city,
    daily_traffic_events.traffic_events,
    daily_traffic_events.minimum_severity,
    daily_traffic_events.low_severity,
    daily_traffic_events.medium_severity,
    daily_traffic_events.high_severity,
    daily_traffic_events.max_severity,
    coalesce(daily_weather_events.weather_events, 0) as weather_events,
    coalesce(daily_weather_events.light, 0) as light_impact,
    coalesce(daily_weather_events.moderate, 0) as moderate_impact,
    coalesce(daily_weather_events.severe, 0) as severe_impact,
    coalesce(daily_weather_events.heavy, 0) as heavy_impact,
    daily_traffic_events.day
    from daily_traffic_events
    left join daily_weather_events 
    ON daily_traffic_events.city=daily_weather_events.city and  daily_traffic_events.day=daily_weather_events.day
)

select * from final
order by day desc