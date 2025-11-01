
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


/*
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
    */

/*
 select distinct
    user_id,
    "timestamp",
    type_id,
    {{ updated_at() }}
from  {{ source("scooters_raw", "events") }}

where
{% if is_incremental() %}
    "timestamp" > (select max("timestamp") from {{ this }})
{% else %}
    "timestamp" < timestamp '2023-08-01'
{% endif %}   

*/

/*
 select 
    user_id,
    "timestamp",
    type_id,
    {{ updated_at() }}
from  {{ source("scooters_raw", "events") }}

where
{% if is_incremental() %}
    "timestamp" > (select max("timestamp") from {{ this }})
{% else %}
    "timestamp" < timestamp '2023-08-01'
{% endif %}  

group by user_id, "timestamp", type_id

*/

{% set date = var("date", none) %}
select distinct
    user_id,
    "timestamp",
    type_id,
    {{ updated_at() }}
from
    {{ source("scooters_raw", "events") }}
where
{% if is_incremental() %}
   
    {% if date %}
        date("timestamp") = date '{{ date }}'
    {% else %}
        "timestamp" > (select max("timestamp") from {{ this }})
    {% endif %}
    
{% else %}
    "timestamp" < timestamp '2023-08-01'
{% endif %}