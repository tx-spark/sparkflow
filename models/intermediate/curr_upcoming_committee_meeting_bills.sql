with upcoming_committee_meeting_bills as (
    select * from {{ source('bills', 'stg_upcoming_committee_meeting_bills') }}
),

-- get the most recent timestamp seen for each bill,
most_recent_timestamp as (
    select 
    max(seen_at) as seen_at_timestamp
    from upcoming_committee_meeting_bills
)

select 
    bill_id,
    leg_id,
    committee,
    chamber,
    meeting_datetime,
    meeting_url,
    link,
    author,
    description,
    status,
    seen_at
from upcoming_committee_meeting_bills
inner join most_recent_timestamp
    on upcoming_committee_meeting_bills.seen_at = most_recent_timestamp.seen_at_timestamp
