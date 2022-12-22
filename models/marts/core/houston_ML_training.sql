with houston_days as (
    select * from {{ref('stg_houston_streets_timestamps')}}
),

houston_hours_traffic as(
    select houston_days.city, houston_days.street, event_timestamp, timezone, type as traffic_event from houston_days
    left join {{source ('traffic_events', 'US_TRAFFIC_EVENTS_RAW')}}
    ON houston_days.street = {{source ('traffic_events', 'US_TRAFFIC_EVENTS_RAW')}}.street and 
    {{source ('traffic_events', 'US_TRAFFIC_EVENTS_RAW')}}.Start_Time_UTC_ < event_timestamp  and 
    {{source ('traffic_events', 'US_TRAFFIC_EVENTS_RAW')}}.End_Time_UTC_ > event_timestamp
),

houston_hours_events as(
    select houston_hours_traffic.city, houston_hours_traffic.street, traffic_event, type as weather_event,  event_timestamp, timezone from houston_hours_traffic
    left join {{source ('weather_events', 'US_WEATHER_EVENTS_RAW')}}
    ON houston_hours_traffic.city = {{source ('weather_events', 'US_WEATHER_EVENTS_RAW')}}.city and
    {{source ('weather_events', 'US_WEATHER_EVENTS_RAW')}}.Start_Time_UTC_ < event_timestamp  and 
    {{source ('weather_events', 'US_WEATHER_EVENTS_RAW')}}.End_Time_UTC_ > event_timestamp
),

houston_hours_events_time_parsed as(
    select city, street, traffic_event, weather_event, 
    CONVERT_TIMEZONE( 'UTC', timezone , event_timestamp) as event_timestamp from houston_hours_events 
)

select * from houston_hours_events_time_parsed