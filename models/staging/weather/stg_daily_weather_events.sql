{{
    config(
      re_data_monitored=true,
      re_data_time_filter='day'
    )
}}
with daily_weather_events as (
    select
        city,
        count(1)  weather_events,
        date_trunc('day', Start_Time_UTC_) as day
    from {{source ('weather_events', 'US_WEATHER_EVENTS_RAW')}}
    where city is not null
    group by city, day
    order by day, city
)

select * from daily_weather_events