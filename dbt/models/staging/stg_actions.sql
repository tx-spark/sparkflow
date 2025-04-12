SELECT
    bill_id,
    leg_id,
    action_number,
    strptime(action_date, '%m/%d/%Y') as action_date,
    description,
    comment,
    case action_timestamp
        when '' then null
        else strptime(action_timestamp, '%m/%d/%Y %I:%M:%S %p')
    end as action_timestamp,
    first_seen_at,
    last_seen_at
FROM {{ source('raw_bills', 'actions') }}

