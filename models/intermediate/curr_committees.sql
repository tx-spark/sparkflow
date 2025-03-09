with committees as (
    select * from {{ source('bills', 'stg_committees') }}
),

-- get the most recent timestamp seen for each bill,
bill_most_recent_timestamp as (
    select 
        bill_id,
        leg_id,
        max(last_seen_at) as seen_at_timestamp
    from committees
    group by 1, 2
),

-- remove all the committees that have dropped off
current_committees AS (
    SELECT
        committees.*
    FROM committees
    inner join bill_most_recent_timestamp
        on committees.last_seen_at = bill_most_recent_timestamp.seen_at_timestamp 
        and committees.bill_id = bill_most_recent_timestamp.bill_id
        and committees.leg_id = bill_most_recent_timestamp.leg_id
)

select
    bill_id,
    leg_id,
    chamber,
    name,
    -- subcommittee_name,
    status,
    -- subcommittee_status,
    aye_votes,
    nay_votes,
    present_votes, 
    absent_votes,
    first_seen_at,
    last_seen_at
from current_committees