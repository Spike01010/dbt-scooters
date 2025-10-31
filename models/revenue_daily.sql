
/*
select
    sum(price_rub) as revenue_rub,
    "date"
from
    {{ ref("trips_prep") }}
{% if is_incremental() %}
where
    "date" >= (select max("date") - interval '2' day from {{ this }})
{% endif %}
group by
    2
order by
    2
*/

select
    sum(price_rub) as revenue_rub,
    "date",
    {{ updated_at() }} 
from
    {{ ref("trips_prep") }}
{% if is_incremental() %}
where
    "date" >= (select max("date") - interval '2' day from {{ this }})
{% endif %}
group by
    2,
    3
order by
    2




dbt_scooters.events_clean
dbt_scooters.revenue_daily
dbt_scooters.trips_concurrency
dbt_scooters.trips_users