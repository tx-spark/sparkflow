SELECT 
    bill_id,
    leg_id,
    author,
    author_type,
    first_seen_at,
    last_seen_at
FROM {{ source('raw_bills', 'authors') }}