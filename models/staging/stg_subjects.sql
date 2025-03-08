SELECT
    bill_id,
    leg_id,
    subject_title,
    subject_id
FROM {{ source('raw_bills', 'subjects') }}