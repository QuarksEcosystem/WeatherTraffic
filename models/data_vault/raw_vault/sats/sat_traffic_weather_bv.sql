SELECT TRAFFIC_WEATHER_PK,
        LOAD_DATE,
        COUNTRY,
        FOOT_TRAFFIC_COUNT,
        NUMBER_OF_LOCATIONS_DESCRIPTION,
        STATE,
        TIMESTAMP,
        TIMESTAMP_INTERVAL,
        TRAFFIC_WEATHER_HASHDIFF,     
        RECORD_SOURCE,
        -- derived additional attributes
        CASE WHEN TOT_PRECIPITATION_IN > 0.3 THEN 'Rain'
                WHEN TOT_SNOWFALL_IN > 0.5 THEN 'SNOW'  
                WHEN AVG_CLOUD_COVER_TOT_PCT > 0.5 THEN 'CLOUDY'
                WHEN AVG_TEMPERATURE_AIR_2M_F < 36 THEN 'COLD'
                ELSE 'CLEAN'
        END WEATHER
  FROM {{ref('sat_traffic_weather')}}