{{
    config(
      re_data_monitored=true
    )
}}
-- select cities with number of monthly traffic and weather events
with monthly_traffic_events as (
    select * from {{ref('stg_monthly_traffic_events_with_severity')}}
) ,
    
monthly_weather_events as (
    select * from {{ref('stg_monthly_weather_events_with_severity')}}
),

final as(
select 
    monthly_traffic_events.city as city,
    monthly_traffic_events.traffic_events,
    monthly_traffic_events.minimum_severity,
    monthly_traffic_events.low_severity,
    monthly_traffic_events.medium_severity,
    monthly_traffic_events.high_severity,
    monthly_traffic_events.max_severity,
    coalesce(monthly_weather_events.weather_events, 0) as weather_events,
    coalesce(monthly_weather_events.light, 0) as light_impact,
    coalesce(monthly_weather_events.moderate, 0) as moderate_impact,
    coalesce(monthly_weather_events.severe, 0) as severe_impact,
    coalesce(monthly_weather_events.heavy, 0) as heavy_impact,
    monthly_traffic_events.month
    from monthly_traffic_events
    left join monthly_weather_events 
    ON monthly_traffic_events.city=monthly_weather_events.city and  monthly_traffic_events.month=monthly_weather_events.month
)

select * from final
order by month desc