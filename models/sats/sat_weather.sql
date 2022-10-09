{%- set source_model = "v_stg_foot_traffic" -%}
{%- set src_pk = "WEATHER_PK" -%}
{%- set src_hashdiff = "WEATHER_HASHDIFF" -%}
{%- set src_payload = ["LDTS", "RSCR", "TIMESTAMP", "TIMESTAMP_INTERVAL"] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}
