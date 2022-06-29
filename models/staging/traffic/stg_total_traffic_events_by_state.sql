{{
    config(
      re_data_monitored=true
    )
}}
with total_traffic_events_by_state as (
    select
        state,
        count(1)  traffic_events
    from {{source ('traffic_events', 'US_TRAFFIC_EVENTS_RAW')}}
    where state is not null
    group by state
)

select * from total_traffic_events_by_state 