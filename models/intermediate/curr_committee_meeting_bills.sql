with committee_meeting_bills as (
    select * from {{ source('bills', 'stg_committee_meeting_bills') }}
),

ranked_committee_meeting_bills AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY bill_id,leg_id, chamber, committee_name  ORDER BY last_seen_at DESC) AS rn
    FROM committee_meeting_bills
)

SELECT
    bill_id, 
    leg_id,
    chamber,
    committee_name,
    meeting_datetime,
    meeting_url,
    first_seen_at,
    last_seen_at
from ranked_committee_meeting_bills
where rn = 1