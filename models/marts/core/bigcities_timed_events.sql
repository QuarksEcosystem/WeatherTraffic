with big_cities_days as (
    select * from {{ref('big_cities_hours')}} limit 10
),

big_cities_hours_traffic as(
    select big_cities_days.city,  to_timestamp(concat(days,' ', hours), 'dd/mm/yyyy hh24:mi:ss') as event_timestamp, type as traffic_events from big_cities_days
    left join {{ref('stg_traffic_parsed_timezone')}}
    ON big_cities_days.city = {{ref('stg_traffic_parsed_timezone')}}.city and 
    {{ref('stg_traffic_parsed_timezone')}}.StartTime < to_timestamp(concat(days,' ', hours), 'dd/mm/yyyy hh24:mi:ss')  and 
    {{ref('stg_traffic_parsed_timezone')}}.EndTime > to_timestamp(concat(days,' ', hours), 'dd/mm/yyyy hh24:mi:ss') 
),

big_cities_hours_events as(
    select big_cities_hours_traffic.city, event_timestamp, traffic_events, type as weather_event from big_cities_hours_traffic
    left join {{ref('stg_weather_parsed_timezone')}}
    ON big_cities_hours_traffic.city = {{ref('stg_weather_parsed_timezone')}}.city and
    {{ref('stg_weather_parsed_timezone')}}.StartTime < event_timestamp  and 
    {{ref('stg_weather_parsed_timezone')}}.EndTime > event_timestamp
)

select * from big_cities_hours_events