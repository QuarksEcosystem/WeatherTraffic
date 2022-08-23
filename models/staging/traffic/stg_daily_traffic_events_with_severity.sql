{{
    config(
      re_data_monitored=true,
      re_data_time_filter='day'
    )
}}
with daily_traffic_events_with_severity as (
    select
        city,
        count(1)  as traffic_events,
        count(case when severity = 0 then 1 else null end) as minimum_severity,
        count(case when severity = 1 then 1 else null end) as low_severity,
        count(case when severity = 2 then 1 else null end) as medium_severity,
        count(case when severity = 3 then 1 else null end) as high_severity,
        count(case when severity = 4 then 1 else null end) as max_severity,
        date_trunc('day', Start_Time_UTC_) as day
    from {{source ('traffic_events', 'US_TRAFFIC_EVENTS_RAW')}}
    where city is not null
    group by city, day
    order by day, city
) 
select * from daily_traffic_events_with_severity