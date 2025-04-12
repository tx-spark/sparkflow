with upcoming_committee_meeting_bills as (
    select * from {{ source('bills', 'stg_upcoming_committee_meeting_bills') }}
),

-- get the most recent timestamp seen for each bill,
most_recent_timestamp as (
    select
    bill_id,
    leg_id,
    min(meeting_datetime) as earliest_meeting_datetime
    from upcoming_committee_meeting_bills
    group by 1,2
)

select 
    upcoming_committee_meeting_bills.bill_id,
    upcoming_committee_meeting_bills.leg_id,
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
    on upcoming_committee_meeting_bills.leg_id = most_recent_timestamp.leg_id
    and upcoming_committee_meeting_bills.bill_id = most_recent_timestamp.bill_id
    and upcoming_committee_meeting_bills.meeting_datetime = most_recent_timestamp.earliest_meeting_datetime
