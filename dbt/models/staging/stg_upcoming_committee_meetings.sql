SELECT
    committee,
    chamber,
    date,
    time,
    location,
    chair,
    meeting_url,
    seen_at
FROM {{ source('raw_bills', 'upcoming_committee_meetings') }}
