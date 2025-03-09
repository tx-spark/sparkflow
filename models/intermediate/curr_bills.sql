with bills as (
    select * from {{ source('bills', 'stg_bills') }}
),

ranked_bills AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id ORDER BY first_seen_at DESC) AS rn
    FROM bills
)

select
    bill_id,
    leg_id,
    caption,
    last_action_date,
    last_action_chamber,
    last_action,
    caption_version,
    first_seen_at,
    last_seen_at
from ranked_bills
where rn = 1