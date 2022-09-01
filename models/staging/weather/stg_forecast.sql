select city:name as city,
city:coord:lat as latitude,
city:coord:lon as longitude, 
concat(city:timezone, ' seconds') as timezone_interval,
l.value:weather[0]:main as weather_event, 
l.value:weather[0]:description as description,
to_timestamp(l.value:dt_txt) as starttime,  
to_timestamp(l.value:dt_txt) + interval '3 hours' as endtime from {{source ('openweatherdata', 'data')}},
lateral flatten(input => list) l