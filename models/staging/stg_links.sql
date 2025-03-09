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
    first_seen_at,
    last_seen_at
FROM {{ source('raw_bills', 'links') }}