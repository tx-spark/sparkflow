with raw_versions as (
    select * from {{ source('raw_bills', 'versions') }}
),

raw_bill_texts as (
    select * from {{ source('raw_bills', 'bill_texts') }}
)

SELECT 
    bill_id,
    leg_id,
    type,
    text_order,
    description,
    html_url,
    pdf_url,
    ftp_html_url,
    raw_versions.ftp_pdf_url,
    raw_bill_texts.text as text,
    first_seen_at,
    last_seen_at
FROM raw_versions
left join raw_bill_texts
    on raw_versions.ftp_pdf_url = raw_bill_texts.ftp_pdf_url