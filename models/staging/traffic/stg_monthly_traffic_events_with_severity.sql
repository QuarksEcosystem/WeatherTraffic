{{
    config(
      re_data_monitored=true,
      re_data_time_filter='month'
    )
}}
with monthly_traffic_events_with_severity as (
    select
        city,
        count(1)  as traffic_events,
        count(case when severity = 0 then 1 else null end) as minimum_severity,
        count(case when severity = 1 then 1 else null end) as low_severity,
        count(case when severity = 2 then 1 else null end) as medium_severity,
        count(case when severity = 3 then 1 else null end) as high_severity,
        count(case when severity = 4 then 1 else null end) as max_severity,
        date_trunc('month', "StartTime(UTC)") as month
    from {{source ('traffic_events', 'US_TRAFFIC_EVENTS_RAW')}}
    where city is not null
    group by city, month
    order by month, city
) 
select * from monthly_traffic_events_with_severity