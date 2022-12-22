{%- set yaml_metadata -%}
source_model: 'raw_stg_foot_traffic'
derived_columns:
  RECORD_SOURCE: 'FILENAME'
  LOAD_DATE: 'LDTS'
  EFFECTIVE_FROM: 'TIMESTAMP'
hashed_columns:
  TRAFFIC_PK: 
    - 'STATE'
    - 'TIMESTAMP'
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

SELECT *
FROM staging