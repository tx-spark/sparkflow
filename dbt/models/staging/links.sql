with links as (
    SELECT
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
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  first_seen_at) as first_seen_at,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  last_seen_at) as last_seen_at
    FROM {{ source('raw_bills', 'links') }}
),

ranked_links AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id ORDER BY last_seen_at DESC) AS rn
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