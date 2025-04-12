

with actions as (
    SELECT
        bill_id,
        leg_id,
        action_number,
        FORMAT_TIMESTAMP('%m/%d/%Y',PARSE_TIMESTAMP('%m/%d/%Y',action_date)) as action_date,
        description,
        comment,
        case action_timestamp
            when '' then null
            else FORMAT_TIMESTAMP('%m/%d/%Y %I:%M:%S %p',PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p',action_timestamp))
        end as action_timestamp,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  first_seen_at) as first_seen_at,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  last_seen_at) as last_seen_at
    FROM {{ source('raw_bills', 'actions') }}
),

-- get the most recent timestamp seen for each bill,
bill_most_recent_timestamp as (
    select 
        bill_id, 
        leg_id,
        max(last_seen_at) as seen_at_timestamp
    from actions
    group by 1, 2
),

-- remove all the actions that have dropped off
current_actions AS (
    SELECT
        actions.*
    FROM actions
    inner join bill_most_recent_timestamp
        on actions.last_seen_at = bill_most_recent_timestamp.seen_at_timestamp 
        and actions.bill_id = bill_most_recent_timestamp.bill_id
        and actions.leg_id = bill_most_recent_timestamp.leg_id
)

select
    bill_id,
    leg_id,
    action_number,
    action_date,
    description,
    comment,
    action_timestamp,
    first_seen_at,
    last_seen_at
from current_actions