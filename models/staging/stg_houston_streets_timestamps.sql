with houston_streets as(
    select city, street, sum(datediff(minute, START_TIME_UTC_, END_TIME_UTC_)) as total_time , TIME_ZONE from {{source ('traffic_events', 'US_TRAFFIC_EVENTS_RAW')}} where city = 'Houston' group by city, street, TIME_ZONE order by total_time desc limit 100
),
houston_days as(
    select * 
    from houston_streets
    cross join {{ ref('DAYS_2016_2020')}}
),

final as(
    select city, street, to_timestamp(concat(days,' ', hours), 'dd/mm/yyyy hh24:mi:ss') as event_timestamp, TIME_ZONE as timezone from houston_days
    cross join {{ ref('hours')}}
)

select * from final