SELECT
    bill_id,
    leg_id,
    caption,
    last_action_date,
    case last_action_chamber
        when 'H' then 'House' 
        when 'S' then 'Senate'
        when 'J' then 'Joint'
        else 'Unknown' end as last_action_chamber,
    last_action,
    caption_version,
    first_seen_at,
    last_seen_at
FROM {{ source('raw_bills', 'bills') }}