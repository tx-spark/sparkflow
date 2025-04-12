with subjects as (
    SELECT
        bill_id,
        leg_id,
        subject_title,
        subject_id,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  first_seen_at) as first_seen_at,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  last_seen_at) as last_seen_at
    FROM {{ source('raw_bills', 'subjects') }}
),

most_recent_timestamp as ( -- I'm assuming that there are no bills without subjects
    select max(last_seen_at) as seen_at_timestamp
    from subjects
),

-- remove all the subjects that have dropped off
current_subjects AS (
    SELECT
        *
    FROM subjects
    inner join most_recent_timestamp
        on subjects.last_seen_at = most_recent_timestamp.seen_at_timestamp 
)

select
    bill_id,
    leg_id,
    subject_title,
    subject_id,
    first_seen_at,
    last_seen_at
from current_subjects