with versions as (
    select * from {{ source('bills', 'stg_versions') }}
),

most_recent_timestamp as ( -- I'm assuming that there are no bills without versions
    select max(last_seen_at) as seen_at_timestamp
    from versions
),

-- remove all the versions that have dropped off
current_versions AS (
    SELECT
        *
    FROM versions
    inner join most_recent_timestamp
        on versions.last_seen_at = most_recent_timestamp.seen_at_timestamp 
)

select
    bill_id,
    leg_id,
    type,
    text_order,
    description,
    html_url,
    pdf_url,
    ftp_html_url,
    ftp_pdf_url,
    first_seen_at,
    last_seen_at
from current_versions