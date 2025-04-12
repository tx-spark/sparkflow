with committee_meeting_bills as (
    SELECT
        REPLACE(bill_id,' ', '') as bill_id, 
        leg_id,
        chamber,
        REPLACE(committee, '&', 'and') as committee_name,
        PARSE_TIMESTAMP('%m/%d/%Y %I:%M %p',
            date || ' ' || 
            case
                when time like '%Canceled/see%' then regexp_replace(substring(time, 1, 8), '[()]', '') -- Remove parentheses and extract time
                when time = '30 minutes' then '12:00 PM' -- Default time for "30 minutes"
                when time like '%upon adjournment%' then '5:00 PM' -- Approximate time for adjournment
                when time like '%upon final%' then '6:00 PM' -- Approximate time for final adjournment
                when time like '%during reading%' then '2:00 PM' -- Approximate time for during reading
                else time
            end
        ) as meeting_datetime,
        concat(
            date, ' ',
            case 
                when time like '%Canceled/see%' then 'Canceled - ' || regexp_replace(substring(time, 1, 8), '[()]', '')
                when time = '30 minutes' then '30 minutes after session'
                when time like '%upon adjournment%' then 'Upon adjournment'
                when time like '%upon final%' then 'Upon final adjournment'
                when time like '%during reading%' then 'During reading & referral'
                else time
            end
        ) as meeting_datetime_text,
        meeting_url,
        status,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  first_seen_at) as first_seen_at,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  last_seen_at) as last_seen_at
    FROM {{ source('raw_bills', 'committee_meeting_bills') }}
    where status = 'scheduled'
),

ranked_committee_meeting_bills AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY bill_id,leg_id, chamber, committee_name  ORDER BY last_seen_at DESC) AS rn
    FROM committee_meeting_bills
)

SELECT
    bill_id, 
    leg_id,
    chamber,
    committee_name,
    meeting_datetime,
    meeting_datetime_text,
    meeting_url,
    status,
    first_seen_at,
    last_seen_at
from ranked_committee_meeting_bills
where rn = 1