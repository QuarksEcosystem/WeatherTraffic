SELECT raw_json:country::varchar as COUNTRY, 
       raw_json:foot_traffic_count_normalized::float AS FOOT_TRAFFIC_COUNT,
       raw_json:number_of_locations_description::varchar AS NUMBER_OF_LOCATIONS_DESCRIPTION,
       raw_json:state::varchar AS STATE,
       FILENAME,
       FILE_ROW_SEQ,
       LDTS,
       RSCR,
       raw_json:timestamp::timestamp AS TIMESTAMP,
       raw_json:timestamp_interval::varchar AS TIMESTAMP_INTERVAL
       FROM {{ source ('datavault_src', 'STG_FOOT_TRAFFIC')}}