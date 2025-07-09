with committees as (
        SELECT
        bill_id,
        leg_id,
            case chamber
            when 'house' then 'House' 
            when 'senate' then 'Senate'
            when 'joint' then 'Joint'
            else 'Unknown' end as chamber,
        REPLACE(name, '&', 'and') as name,
        -- subcommittee_name,
        status,
        -- subcommittee_status,
        aye_votes,
        nay_votes,
        present_votes, 
        absent_votes,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  first_seen_at) as first_seen_at,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  last_seen_at) as last_seen_at
    FROM {{ source('raw_bills', 'committee_status') }}
    where name != ''
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