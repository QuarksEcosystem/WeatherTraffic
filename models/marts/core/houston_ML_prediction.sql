with houston_streets as(
    select city, street, sum(datediff(minute, START_TIME_UTC_, END_TIME_UTC_)) as total_time , TIME_ZONE from {{source ('traffic_events', 'US_TRAFFIC_EVENTS_RAW')}} where city = 'Houston' group by city, street, TIME_ZONE order by total_time desc limit 100
),

houston_d as(
    select distinct houston_streets.city, houston_streets.street, houston_streets.TIME_ZONE, to_date(Starttime) as days
    from houston_streets
    cross join {{ref('stg_forecast')}}
),

houston_days as(
    select city, street, to_timestamp(concat(days,' ', hours), 'yyyy-mm-dd hh24:mi:ss') as event_timestamp, TIME_ZONE as timezone from houston_d
    cross join {{ ref('hours')}}
),

houston_hours_traffic as(
    select houston_days.city, houston_days.street, event_timestamp, timezone, type as traffic_event from houston_days
    left join {{source ('traffic_events', 'US_TRAFFIC_EVENTS_RAW')}}
    ON houston_days.street = {{source ('traffic_events', 'US_TRAFFIC_EVENTS_RAW')}}.street and 
    {{source ('traffic_events', 'US_TRAFFIC_EVENTS_RAW')}}.Start_Time_UTC_ < event_timestamp  and 
    {{source ('traffic_events', 'US_TRAFFIC_EVENTS_RAW')}}.End_Time_UTC_ > event_timestamp
),

houston_hours_events as(
    select houston_hours_traffic.city, houston_hours_traffic.street, traffic_event, weather_event,  event_timestamp, timezone from houston_hours_traffic
    left join {{ref('stg_forecast')}}
    ON houston_hours_traffic.city = {{ref('stg_forecast')}}.city and
    {{ref('stg_forecast')}}.Starttime < event_timestamp  and 
    {{ref('stg_forecast')}}.EndTime > event_timestamp
),

houston_hours_events_time_parsed as(
    select city, street, traffic_event, weather_event, 
    CONVERT_TIMEZONE( 'UTC', timezone , event_timestamp) as event_timestamp from houston_hours_events 
)

select * from houston_hours_events_time_parsed