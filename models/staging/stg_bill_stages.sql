SELECT 
    bill_id,
    leg_id,
    TRY_CAST(RIGHT(stage, 1) AS INTEGER) AS stage_num, -- IF there's a point where the tx leg changes and there are over 10 stages, we'll need to change this
    stage_title,
    stage_text,
    IF(stage_date == '*See below.', NULL, strptime(stage_date, '%m/%d/%Y')) as stage_date,
    if (
        div_class = 'complete' and after_status = 'fail', 'Dead', 
        if (div_class = 'failed', 'Dead','Alive')
    ) as status,
    first_seen_at,
    last_seen_at
FROM {{ source('raw_bills', 'bill_stages') }}
