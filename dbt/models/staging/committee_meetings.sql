with committee_meetings as (
    SELECT
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
                when lower(time) like '%upon lunch%' then '1:00 PM'
                when lower(time) like '%upon%' then '1:00 PM'
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
                when lower(time) like '%upon lunch%' then 'Upon lunch'
                when lower(time) like '%upon%' then time
                else time
            end
        ) as meeting_datetime_text,
        location,
        chair,
        meeting_url,
        committee_meetings_link,
        right(committee_meetings_link,4) as committee_code,
        hearing_notice_html,
        hearing_notice_pdf,
        minutes_html,
        minutes_pdf,
        witness_list_html,
        witness_list_pdf,
        comments,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  first_seen_at) as first_seen_at,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  last_seen_at) as last_seen_at
    FROM {{ source('raw_bills', 'committee_meetings') }}
),

ranked_committee_meetings AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY leg_id, chamber, committee_name, meeting_datetime ORDER BY last_seen_at DESC) AS rn
    FROM committee_meetings
)

SELECT
    leg_id,
    chamber,
    committee_name,
    meeting_datetime,
    meeting_datetime_text,
    location,
    chair,
    meeting_url,
    committee_meetings_link,
    committee_code,
    hearing_notice_html,
    hearing_notice_pdf,
    minutes_html,
    minutes_pdf,
    witness_list_html,
    witness_list_pdf,
    comments,
    first_seen_at,
    last_seen_at
from ranked_committee_meetings
where rn = 1