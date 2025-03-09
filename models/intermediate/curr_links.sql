with links as (
    select * from {{ source('bills', 'stg_links') }}
),

ranked_links AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id ORDER BY first_seen_at DESC) AS rn
    FROM links
)

select
    bill_id,
    leg_id,
    history,
    text,
    actions,
    companions,
    amendments,
    authors,
    sponsors,
    captions,
    bill_stages,
    first_seen_at,
    last_seen_at
from ranked_links
where rn = 1