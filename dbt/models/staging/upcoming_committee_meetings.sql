with upcoming_committee_meetings as (
    SELECT
        committee,
        chamber,
        date,
        time,
        location,
        chair,
        meeting_url,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  seen_at) as seen_at
    FROM {{ source('raw_bills', 'upcoming_committee_meetings') }}
),

-- get the most recent timestamp seen for each meeting
most_recent_timestamp as (
    select 
    max(seen_at) as seen_at_timestamp
    from upcoming_committee_meetings
)

select 
    committee,
    chamber,
    date,
    time,
    location,
    chair,
    meeting_url,
    seen_at
from upcoming_committee_meetings
inner join most_recent_timestamp
    on upcoming_committee_meetings.seen_at = most_recent_timestamp.seen_at_timestamp
