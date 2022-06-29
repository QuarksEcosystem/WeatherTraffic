{{
    config(
      re_data_monitored=true,
      re_data_time_filter='day'
    )
}}
with daily_weather_events_with_severity as (
    select
        city,
        count(1)  weather_events,
        count(case when severity = 'Light' then 1 else null end) as light,
        count(case when severity = 'Moderate' then 1 else null end) as moderate,
        count(case when severity = 'Severe' then 1 else null end) as severe,
        count(case when severity = 'Heavy' then 1 else null end) as heavy,
        date_trunc('day', StartTime) as day
    from {{source ('weather_events', 'US_WEATHER_EVENTS_RAW')}}
    where city is not null
    group by city, day
    order by day, city
)
select * from daily_weather_events_with_severity