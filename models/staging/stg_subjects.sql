SELECT
    bill_id,
    leg_id,
    subject_title,
    subject_id,
    first_seen_at,
    last_seen_at
FROM {{ source('raw_bills', 'subjects') }}
