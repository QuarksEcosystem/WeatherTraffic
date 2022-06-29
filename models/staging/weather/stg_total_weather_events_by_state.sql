{{
    config(
      re_data_monitored=true
    )
}}
with total_weather_events_by_state as (
    select
        state,
        count(1)  weather_events
    from {{source ('weather_events', 'US_WEATHER_EVENTS_RAW')}}
    where state is not null
    group by state
)

select * from total_weather_events_by_state 