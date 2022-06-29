{{
    config(
      re_data_monitored=true
    )
}}
with total_traffic_events_by_city as (
    select
        city,
        count(1)  traffic_events
    from {{source ('traffic_events', 'US_TRAFFIC_EVENTS_RAW')}}
    where city is not null
    group by city
)

select * from total_traffic_events_by_city