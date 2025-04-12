with committee_hearing_videos as (
    SELECT
    date,
    time,
    regexp_replace(
        if(chamber = 'Senate', 
            replace(replace(program,'&', 'and'), 'Senate Committee on ', ''),
            replace(program,'&', 'and')
        ),
        ' [(]Part [I|V]+[)]$', ''
    ) as program,
    nullif(regexp_extract(program, '[(]Part ([I|V]+)[)]', 1), '') as part,
    video_link,
    chamber,
    leg_id,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  first_seen_at) as first_seen_at,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  last_seen_at) as last_seen_at
FROM {{ source('raw_bills', 'committee_hearing_videos') }}
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