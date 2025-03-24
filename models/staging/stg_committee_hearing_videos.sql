SELECT
    date,
    time,
    regexp_replace(
        if(chamber = 'Senate', 
            replace(replace(program,'&', 'and'), 'Senate Committee on ', ''),
            replace(program,'&', 'and')
        ),
        ' \(Part [I|V]+\)$', ''
    ) as program,
    nullif(regexp_extract(program, '\(Part ([I|V]+)\)', 1), '') as part,
    video_link,
    chamber,
    leg_id,
    first_seen_at,
    last_seen_at
FROM {{ source('raw_bills', 'committee_hearing_videos') }}