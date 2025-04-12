select
    REPLACE(bill_id, ' ', '') as bill_id,
    leg_id,
    committee,
    chamber,
    strptime(date || ' ' || time, '%m/%d/%Y %I:%M %p') as meeting_datetime,
    meeting_url,
    link,
    author,
    description,
    status,
    seen_at
from {{ source('raw_bills', 'upcoming_committee_meeting_bills') }}
