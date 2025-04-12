SELECT
    bill_id,
    leg_id,
    REPLACE(companion_bill_id,' ','') as companion_bill_id,
    relationship,
    first_seen_at,
    last_seen_at
FROM {{ source('raw_bills', 'companions') }}
