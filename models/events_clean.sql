
/*
select distinct
    user_id,
    "timestamp",
    type_id
from
    {{ source("scooters_raw", "events") }}
where
{% if is_incremental() %}
    "timestamp" > (select max("timestamp") from {{ this }})
{% else %}
    "timestamp" < timestamp '2023-08-01'
{% endif %}
*/



with row_num_cte as (
    select
        user_id,
        "timestamp",
        type_id,
        row_number() over (partition by user_id, "timestamp", type_id) as row_num
    from
        {{ source("scooters_raw", "events") }}
)
select
    user_id,
    "timestamp",
    type_id
from
    row_num_cte
where
    row_num = 1