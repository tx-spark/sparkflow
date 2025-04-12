with authors as (
    select * from {{ source('bills', 'stg_authors') }}
),

-- get the most recent timestamp seen for each bill,
bill_most_recent_timestamp as (
    select 
        bill_id,
        leg_id,
        max(last_seen_at) as seen_at_timestamp
    from authors
    group by 1, 2
),

-- remove all the authors that have dropped off
current_authors AS (
    SELECT
        authors.*
    FROM authors
    inner join bill_most_recent_timestamp
        on authors.last_seen_at = bill_most_recent_timestamp.seen_at_timestamp 
        and authors.bill_id = bill_most_recent_timestamp.bill_id
        and authors.leg_id = bill_most_recent_timestamp.leg_id
)

select
    bill_id,
    leg_id,
    author,
    author_type,
    first_seen_at,
    last_seen_at
from current_authors