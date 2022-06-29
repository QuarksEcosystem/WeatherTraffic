{{
    config(
      re_data_monitored=true
    )
}}
with monthly_weather_events as (
    select
        city,
        count(1)  weather_events,
        date_trunc('month', StartTime) as month
    from {{source ('weather_events', 'US_WEATHER_EVENTS_RAW')}}
    where city is not null and (DATE_PART(hh, StartTime) > 05 and DATE_PART(hh, StartTime) < 23) and not severity='light'
    group by city, month
    order by month, city
)

select * from monthly_weather_events