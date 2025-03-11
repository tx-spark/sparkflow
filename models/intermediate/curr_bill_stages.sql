with bill_stages as (
    select * from {{ source('bills', 'stg_bill_stages') }}
),

-- get the most recent timestamp seen for each bill,
bill_most_recent_timestamp as (
    select 
        bill_id,
        leg_id,
        max(last_seen_at) as seen_at_timestamp
    from bill_stages
    group by 1, 2
),

-- remove all the bill_stages that have dropped off
current_bill_stages AS (
    SELECT
        bill_stages.*
    FROM bill_stages
    inner join bill_most_recent_timestamp
        on bill_stages.last_seen_at = bill_most_recent_timestamp.seen_at_timestamp 
        and bill_stages.bill_id = bill_most_recent_timestamp.bill_id
        and bill_stages.leg_id = bill_most_recent_timestamp.leg_id
)

select
    bill_id,
    leg_id,
    stage_num, -- IF there's a point where the tx leg changes and there are over 10 stages, we'll need to change this
    stage_title,
    stage_text,
    stage_date,
    status,
    first_seen_at,
    last_seen_at
from current_bill_stages