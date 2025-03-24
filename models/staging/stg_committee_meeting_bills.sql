SELECT
    REPLACE(bill_id,' ', '') as bill_id, 
    leg_id,
    chamber,
    REPLACE(committee, '&', 'and') as committee_name,
    strptime(date || ' ' || time, '%m/%d/%Y %I:%M %p') as meeting_datetime,
    meeting_url,
    first_seen_at,
    last_seen_at
FROM {{ source('raw_bills', 'committee_meeting_bills') }}
where status = 'scheduled'