SELECT
    bill_id,
    leg_id,
        case chamber
        when 'house' then 'House' 
        when 'senate' then 'Senate' 
        else 'Unknown' end as chamber,
    name,
    -- subcommittee_name,
    status,
    -- subcommittee_status,
    aye_votes,
    nay_votes,
    present_votes, 
    absent_votes
FROM {{ source('raw_bills', 'committees') }}