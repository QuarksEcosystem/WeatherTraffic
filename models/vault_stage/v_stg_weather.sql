{%- set yaml_metadata -%}
source_model:
    weather_src: 'HISTORY_DAY'
derived_columns:
  RECORD_SOURCE: '!SHARED_WEATHER_DATAFILE'
  EFFECTIVE_FROM: 'DATE_VALID_STD'
hashed_columns:
  WEATHER_PK: 
    - 'POSTAL_CODE'
    - 'DATE_VALID_STD'
  WEATHER_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'POSTAL_CODE'
      - 'DATE_VALID_STD'
      - 'AVG_RADIATION_SOLAR_TOTAL_WPM2'
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