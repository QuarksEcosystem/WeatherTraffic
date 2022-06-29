{{
    config(
      re_data_monitored=true
    )
}}
-- select cities with number of daily traffic and weather events

with monthly_traffic_events as (
    select * from {{ref('stg_monthly_traffic_events')}}
),

monthly_weather_events as (
    select * from {{ref('stg_monthly_weather_events')}}
),

final as(
select 
    monthly_traffic_events.city as city,
    monthly_traffic_events.traffic_events,
    coalesce(monthly_weather_events.weather_events, 0) as weather_events,
    monthly_traffic_events.month
    from monthly_traffic_events
    left join monthly_weather_events 
    ON monthly_traffic_events.city=monthly_weather_events.city and  monthly_traffic_events.month=monthly_weather_events.month
)

select * from final