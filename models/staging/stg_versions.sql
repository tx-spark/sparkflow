SELECT 
    bill_id,
    leg_id,
    type,
    description,
    html_url,
    pdf_url,
    ftp_html_url,
    ftp_pdf_url,
    first_seen_at,
    last_seen_at
FROM {{ source('raw_bills', 'versions') }}