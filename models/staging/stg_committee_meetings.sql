SELECT
    leg_id,
    REPLACE(name, '&', 'and') as name,
    case chamber
        when 'H' then 'House' 
        when 'S' then 'Senate' 
        when 'J' then 'Joint'
        else 'Unknown' end as chamber,
    link,
    first_seen_at,
    last_seen_at
FROM {{ source('raw_bills', 'committee_meetings') }}