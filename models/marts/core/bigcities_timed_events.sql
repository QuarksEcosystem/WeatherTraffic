with big_cities_days as (
    select * from {{ref('big_cities_hours')}}
),

big_cities_hours_traffic as(
    select big_cities_days.city,  to_timestamp(concat(days,' ', hours), 'dd/mm/yyyy hh24:mi:ss') as event_timestamp, eventid as traffic_event_id, type as traffic_event, StartTime from big_cities_days
    left join {{ref ('stg_traffic_parsed_timezone')}}
    ON big_cities_days.city = {{ref ('stg_traffic_parsed_timezone')}}.city and abs(datediff(minute, {{ref ('stg_traffic_parsed_timezone')}}.StartTime, to_timestamp(concat(days,' ', hours), 'dd/mm/yyyy hh24:mi:ss') ))<= 7
),
big_cities_hours_events as(
    select big_cities_hours_traffic.city, event_timestamp, traffic_event_id, traffic_event, type as weather_event from big_cities_hours_traffic
    left join {{ref ('stg_weather_parsed_timezone')}}
    ON big_cities_hours_traffic.city = {{ref ('stg_weather_parsed_timezone')}}.city and abs(datediff(minute, {{ref ('stg_weather_parsed_timezone')}}.StartTime, event_timestamp) )<= 7
)

select * from big_cities_hours_events