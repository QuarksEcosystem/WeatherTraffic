{%- set source_model = "v_stg_traffic_weather_tx" -%}
{%- set src_pk = "TRAFFIC_WEATHER_PK" -%}
{%- set src_fk = ["TRAFFIC_PK", "WEATHER_PK"] -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                 src_source=src_source, source_model=source_model) }}
