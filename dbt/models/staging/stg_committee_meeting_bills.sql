SELECT
    REPLACE(bill_id,' ', '') as bill_id, 
    leg_id,
    chamber,
    REPLACE(committee, '&', 'and') as committee_name,
        strptime(
        date || ' ' || 
        case
            when time like '%Canceled/see%' then regexp_replace(substring(time, 1, 8), '[()]', '') -- Remove parentheses and extract time
            when time = '30 minutes' then '12:00 PM' -- Default time for "30 minutes"
            when time like '%upon adjournment%' then '5:00 PM' -- Approximate time for adjournment
            when time like '%upon final%' then '6:00 PM' -- Approximate time for final adjournment
            when time like '%during reading%' then '2:00 PM' -- Approximate time for during reading
            else time
        end,
        '%m/%d/%Y %I:%M %p'
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
    first_seen_at,
    last_seen_at
FROM {{ source('raw_bills', 'committee_meeting_bills') }}
where status = 'scheduled'