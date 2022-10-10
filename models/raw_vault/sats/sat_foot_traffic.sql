{%- set source_model = "v_stg_foot_traffic" -%}
{%- set src_pk = "TRAFFIC_PK" -%}
{%- set src_hashdiff = "TRAFFIC_HASHDIFF" -%}
{%- set src_payload = ["COUNTRY", "FOOT_TRAFFIC_COUNT", "NUMBER_OF_LOCATIONS_DESCRIPTION", "STATE",
                       "FILENAME", "FILE_ROW_SEQ", "LDTS", "RSCR", "TIMESTAMP", "TIMESTAMP_INTERVAL"] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}
