select
    REPLACE(bill_id, ' ', '') as bill_id,
    leg_id,
    committee,
    chamber,
    PARSE_TIMESTAMP('%m/%d/%Y %I:%M %p', CONCAT(date, ' ', time)) as meeting_datetime,
    meeting_url,
    link,
    author,
    description,
    status,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  seen_at) as seen_at
from {{ source('raw_bills', 'upcoming_committee_meeting_bills') }}
