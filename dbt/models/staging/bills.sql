with bills as (
    SELECT
        bill_id,
        leg_id,
        caption,
        last_action_date,
        case last_action_chamber
            when 'H' then 'House' 
            when 'S' then 'Senate'
            when 'J' then 'Joint'
            when 'E' then 'Executive'
            else 'Unknown' end as last_action_chamber,
        replace(last_action, '. . .', last_action_date) as last_action,
        caption_version,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  first_seen_at) as first_seen_at,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  last_seen_at) as last_seen_at
    FROM {{ source('raw_bills', 'bills') }}
),

ranked_bills AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id ORDER BY last_seen_at DESC) AS rn
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