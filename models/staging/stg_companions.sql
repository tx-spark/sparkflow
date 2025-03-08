SELECT
    bill_id,
    leg_id,
    REPLACE(companion_bill_id,' ','') as companion_bill_id,
    relationship
FROM {{ source('raw_bills', 'companions') }}
