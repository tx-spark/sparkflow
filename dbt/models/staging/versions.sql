with raw_versions as (
    select * from {{ source('raw_bills', 'versions') }}
),

raw_bill_texts as (
    select * from {{ source('raw_bills', 'bill_texts') }}
),
versions as (
    SELECT 
        raw_versions.bill_id,
        raw_versions.leg_id,
        type,
        text_order,
        description,
        html_url,
        pdf_url,
        ftp_html_url,
        raw_versions.ftp_pdf_url,
        raw_bill_texts.text as text,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  first_seen_at) as first_seen_at,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  last_seen_at) as last_seen_at
    FROM raw_versions
    left join raw_bill_texts
        on raw_versions.ftp_pdf_url = raw_bill_texts.ftp_pdf_url
),

most_recent_timestamp as ( -- I'm assuming that there are no bills without versions
    select leg_id,max(last_seen_at) as seen_at_timestamp
    from versions
    group by leg_id
),

-- remove all the versions that have dropped off

most_recent_versions as (
    SELECT -- remove all the versions that have dropped off
        bill_id,
        versions.leg_id,
        type,
        text_order,
        description,
        html_url,
        pdf_url,
        ftp_html_url,
        ftp_pdf_url,
        text,
        first_seen_at,
        last_seen_at,
        row_number() over (PARTITION BY bill_id, versions.leg_id, type, text_order, description, html_url, pdf_url, ftp_html_url, ftp_pdf_url, first_seen_at, last_seen_at) as rn
    FROM versions
    inner join most_recent_timestamp
        on versions.last_seen_at = most_recent_timestamp.seen_at_timestamp 
        and versions.leg_id = most_recent_timestamp.leg_id
)

select * except(rn) from most_recent_versions where rn = 1

