with complete_bills_list as (
    select * from {{ source('bills', 'stg_complete_bills_list') }}
),

ranked_complete_bills_list AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id ORDER BY last_seen_at DESC) AS rn
    FROM complete_bills_list
)

select
    bill_id,
    leg_id,
    first_seen_at,
    last_seen_at
from ranked_complete_bills_list
where rn = 1
order by leg_id, bill_id