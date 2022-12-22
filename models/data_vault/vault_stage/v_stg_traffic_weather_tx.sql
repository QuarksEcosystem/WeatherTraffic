{%- set yaml_metadata -%}
source_model: 'raw_stg_traffic_weather_tx'
derived_columns:
  EFFECTIVE_FROM: 'TIMESTAMP'
  RECORD_SOURCE: 'FILENAME'
hashed_columns:
  TRAFFIC_WEATHER_PK:
    - 'POSTAL_CODE'
    - 'DATE_VALID_STD'
    - 'STATE'
    - 'TIMESTAMP'
  WEATHER_PK: 
    - 'POSTAL_CODE'
    - 'DATE_VALID_STD'
  TRAFFIC_PK: 
    - 'STATE'
    - 'TIMESTAMP'
  TRAFFIC_WEATHER_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'COUNTRY'
      - 'STATE'
      - 'FOOT_TRAFFIC_COUNT'
      - 'NUMBER_OF_LOCATIONS_DESCRIPTION'
      - 'TIMESTAMP'
      - 'POSTAL_CODE'
      - 'DATE_VALID_STD'
      - 'AVG_RADIATION_SOLAR_TOTAL_WPM2'
  WEATHER_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'POSTAL_CODE'
      - 'DATE_VALID_STD'
      - 'AVG_RADIATION_SOLAR_TOTAL_WPM2'
  TRAFFIC_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'COUNTRY'
      - 'STATE'
      - 'FOOT_TRAFFIC_COUNT'
      - 'NUMBER_OF_LOCATIONS_DESCRIPTION'
      - 'TIMESTAMP'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}


WITH staging AS (
{{ dbtvault.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=none) }}
)

SELECT *, CURRENT_TIME() AS LOAD_DATE
FROM staging