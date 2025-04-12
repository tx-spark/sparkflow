with subjects as (
    select * from {{ source('bills', 'stg_subjects') }}
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