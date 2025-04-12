with committee_hearing_videos as (
    select * from {{ source('bills', 'stg_committee_hearing_videos') }}
),

ranked_committee_hearing_videos AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY leg_id, date, time, program, chamber ORDER BY last_seen_at DESC) AS rn
    FROM committee_hearing_videos
)

SELECT
    date,
    time,
    program,
    part,
    video_link,
    chamber,
    leg_id,
    first_seen_at,
    last_seen_at
from ranked_committee_hearing_videos
where rn = 1