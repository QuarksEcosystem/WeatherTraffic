with big_cities as (
    select
        city
    from {{ ref('uscitypopdensity')}}
        order by Population desc
        limit 10
),

cities_days as(
    select * from big_cities
    cross join {{ ref('DAYS_2016_2020')}}
),

final as(
    select * from cities_days
    cross join {{ ref('hours')}}
)

select * from final