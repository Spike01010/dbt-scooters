
  
  with
unnest_cte as (
    -- Unnest trip to two rows: start and finish events
    select
        unnest(array[started_at, finished_at]) as "timestamp",
        unnest(array[1, -1]) as increment
    from
        "postgres"."scooters_raw"."trips"
),
sum_cte as (
    -- Make timestamp unique, group increments
    select
        "timestamp",
        sum(increment) as increment
    from
        unnest_cte
    where
    
        "timestamp" < (date '2023-06-01' + interval '7' hour) at time zone 'Europe/Moscow'
    
    group by
        1
),
cumsum_cte as (
    -- Integrate increment to get concurrency
    select
        "timestamp",
        sum(increment) over (order by "timestamp") as concurrency
    from
        sum_cte
)
select
    "timestamp",
    concurrency
from
    cumsum_cte
order by
    1