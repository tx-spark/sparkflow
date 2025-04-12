SELECT
    bill_id,
    leg_id,
    first_seen_at,
    last_seen_at
FROM {{ source('raw_bills', 'complete_bills_list') }}