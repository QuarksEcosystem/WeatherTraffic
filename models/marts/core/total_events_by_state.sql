{{
    config(
      re_data_monitored=true
    )
}}
with total_traffic_events_by_state as (
    select * from {{ref('stg_total_traffic_events_by_state')}}
),

total_weather_events_by_state as (
    select * from {{ref('stg_total_weather_events_by_state')}}
),

final as(
select 
    total_traffic_events_by_state.state as STUSPS,
    total_traffic_events_by_state.traffic_events,
    total_weather_events_by_state.weather_events
    from total_traffic_events_by_state
    left join total_weather_events_by_state
    ON total_traffic_events_by_state.state=total_weather_events_by_state.state
)

select * from final