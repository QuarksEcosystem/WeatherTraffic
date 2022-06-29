with big_cities as (
    select
        city
    from {{ref ('uscitypopdensity')}}
        order by traffic_events desc
        limit 200
),
hour as (
    select hours from {{ref ('hours')}}
),
day as(
    select day from {{ref ('DAYS_2018_2020')}}
)
