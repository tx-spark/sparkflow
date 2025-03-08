SELECT
    bill_id,
    leg_id,
    caption,
    last_action_date,
    case last_action_chamber
        when 'H' then 'House' 
        when 'S' then 'Senate' 
        else 'Unknown' end as last_action_chamber,
    last_action,
    caption_version
FROM {{ source('raw_bills', 'bills') }}