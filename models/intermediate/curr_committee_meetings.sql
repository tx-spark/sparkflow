with committee_meetings as (
    select * from {{ source('bills', 'stg_committee_meetings') }}
),

ranked_committee_meetings AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY leg_id, name ORDER BY last_seen_at DESC) AS rn
    FROM committee_meetings
)

select
    leg_id,
    name,
    chamber,
    link,
    first_seen_at,
    last_seen_at
from ranked_committee_meetings
where rn = 1