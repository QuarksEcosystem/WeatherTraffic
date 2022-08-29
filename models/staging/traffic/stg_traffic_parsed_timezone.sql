select EVENT_ID AS EVENTID, TYPE, SEVERITY, TMC, DESCRIPTION, CONVERT_TIMEZONE( 'UTC', TIME_ZONE , START_TIME_UTC_ ) as StartTime,
       CONVERT_TIMEZONE( 'UTC', TIME_ZONE , END_TIME_UTC_ ) as EndTime, LOCATION_LAT, LOCATION_LNG,
       DISTANCE_MI_, AIRPORT_CODE, NUMBER, STREET, SIDE, CITY, COUNTY, STATE, ZIP_CODE from {{source ('traffic_events', 'US_TRAFFIC_EVENTS_RAW')}}