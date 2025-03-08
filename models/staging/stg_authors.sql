SELECT 
    bill_id,
    leg_id,
    author,
    author_type
FROM {{ source('raw_bills', 'authors') }}