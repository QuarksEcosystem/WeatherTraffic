{{
    config(
      re_data_monitored=true,
      re_data_time_filter='day'
    )
}}

with daily_traffic_events as (
    select * from {{ref('stg_daily_traffic_events')}}
),

holidays as (
select * from {{ref ('US_Holiday_Dates_2004_2021')}}
),

final as(
select 
    daily_traffic_events.city as city,
    daily_traffic_events.traffic_events,
    holidays.holiday,
    daily_traffic_events.day
    from daily_traffic_events
    left join holidays
    ON daily_traffic_events.day=holidays.date
)

select * from final