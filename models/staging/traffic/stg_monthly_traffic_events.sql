{{
    config(
      re_data_monitored=true,
      re_data_time_filter='month'
    )
}}
with monthly_traffic_events as (
    select
        city,
        count(1)  as traffic_events,
        date_trunc('month', "StartTime(UTC)") as month
    from {{source ('traffic_events', 'US_TRAFFIC_EVENTS_RAW')}}
    where city is not null
    group by city, month
    order by month, city
)
select * from monthly_traffic_events