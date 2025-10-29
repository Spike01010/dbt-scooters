select
    t.*,
    u.sex,
    extract(year from t.started_at) - extract(year from u.birth_date) as age
from
    {{ ref("trips_prep") }} as t
left join
    {{ source("scooters_raw", "users") }} as u
    on t.user_id = u.id