with committee_meetings as (
    select * from {{ source('bills', 'stg_committee_meetings') }}
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
    location,
    chair,
    meeting_url,
    committee_meetings_link,
    meeting_url,
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