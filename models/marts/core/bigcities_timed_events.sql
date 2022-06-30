with big_cities_days as (
    select * from {{ref('big_cities_hours')}}
),

big_cities_hours_traffic as(
    select big_cities_days.city,  to_timestamp(concat(days,' ', hours), 'dd/mm/yyyy hh24:mi:ss') as event_timestamp, eventid as traffic_event_id, type as traffic_event, "StartTime(UTC)" from big_cities_days
    left join TRIGGO_EXAMPLES_DATABASE.traffic.US_TRAFFIC_EVENTS_RAW
    ON big_cities_days.city = TRIGGO_EXAMPLES_DATABASE.traffic.US_TRAFFIC_EVENTS_RAW.city and abs(datediff(minute, TRIGGO_EXAMPLES_DATABASE.traffic.US_TRAFFIC_EVENTS_RAW."StartTime(UTC)", to_timestamp(concat(days,' ', hours), 'dd/mm/yyyy hh24:mi:ss') ))<= 7
),
big_cities_hours_events as(
    select big_cities_hours_traffic.city, event_timestamp, traffic_event_id, traffic_event, type as weather_event, StartTime from big_cities_hours_traffic
    left join TRIGGO_EXAMPLES_DATABASE.weather.US_WEATHER_EVENTS_RAW
    ON big_cities_hours_traffic.city = TRIGGO_EXAMPLES_DATABASE.weather.US_WEATHER_EVENTS_RAW.city and abs(datediff(minute, TRIGGO_EXAMPLES_DATABASE.weather.US_WEATHER_EVENTS_RAW.StartTime, event_timestamp) )<= 7
)

select * from big_cities_hours_events;