with bills as (
    select * from {{ source('bills', 'curr_bills') }}
),

links as (
    select * from {{ source('bills', 'curr_links') }}
),

authors as (
    select * from {{ source('bills', 'curr_authors') }}
),

actions as (
    select * from {{ source('bills', 'curr_actions') }}
),

versions as (
    select * from {{ source('bills', 'curr_versions') }}
),

companions as (
    select * from {{ source('bills', 'curr_companions') }}
),

committee_meetings as (
    select * from {{ source('bills', 'curr_committee_meetings') }}
),

----------------------------------------------------------

authors_agg as (
    SELECT
        bill_id,
        leg_id,
        STRING_AGG(authors.author, ' | ') as authors_list
    FROM
        authors
    GROUP BY
        bill_id,
        leg_id
),

introduced_versions as (
    select * from versions
    where description = 'Introduced'
    and type = 'Bill'
),

-- Get the most recent version for each bill
most_recent_versions as (
    SELECT * exclude (rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id ORDER BY text_order DESC) as rn
        FROM versions
    ) v
    WHERE rn = 1
    and type = 'Bill'
),

companions_agg as (
    SELECT
        bill_id,
        leg_id,
        STRING_AGG(companions.companion_bill_id, ', ') as companions_list
    FROM
        companions
    GROUP BY
        bill_id,
        leg_id
),
    
most_recent_companion as (
    SELECT c.* exclude (rn), 
    links.history as companion_history
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id ORDER BY last_seen_at) as rn
        FROM companions
    ) c 
    left join links
        on c.companion_bill_id = links.bill_id
        and c.leg_id = links.leg_id
    WHERE rn = 1
)
----------------------------------------------------------

select
    bills.bill_id,
    bills.leg_id,
    bills.caption,
    -- Bill History/Status
    CONCAT(
        '=HYPERLINK("',
        links.authors,
        '", "',
        authors_agg.authors_list,
        '")'
    ) as authors,   
    links.captions, -- caption link
    -- Dead|Alive|Unassigned|Law
    bills.last_action_date, -- last action date
    bills.last_action_chamber, -- last action chamber
    CONCAT(
        '=HYPERLINK("',
        links.actions,
        '", "',
        bills.last_action,
        '")'
    ) as last_action,  
    links.text, --Link | All Texts
    most_recent_versions.pdf_url, -- Recent bill text link
    introduced_versions.pdf_url, -- Introduced bill text link
    CONCAT(
        '=HYPERLINK("',
        most_recent_versions.pdf_url,
        '", "',
        'Recent Bill Text',
        '")'
    ) as recent_bill_text,
    CONCAT(
        '=HYPERLINK("',
        most_recent_companion.companion_history,
        '", "',
        companions_agg.companions_list,
        '")'
    ) as recent_bill_text,
    links.amendments as amendments,
    NULL as sponsors -- MONITOR SPONSORS, IF DATA STARTS TO COME IN, ADD THIS COLUMN
    -- Companions =HYPERLINK("https://capitol.texas.gov/BillLookup/History.aspx?LegSess=89R&Bill=HB3", "HB 3")
    -- Amendments =HYPERLINK("https://capitol.texas.gov/BillLookup/Amendments.aspx?LegSess=89R&Bill=SB2", "37")
    -- Sponsors =https://capitol.texas.gov/BillLookup/Sponsors.aspx?LegSess=89R&Bill=SB2
    -- Bill Stage Hyperlink to current stage
    -- Committee Hyperlink to current committee
    -- Current Version =HYPERLINK("https://capitol.texas.gov/BillLookup/Versions.aspx?LegSess=89R&Bill=HB3", "HB 3")
    
from bills
left join links
    on bills.bill_id = links.bill_id
    and bills.leg_id = links.leg_id
left join authors_agg
    on bills.bill_id = authors_agg.bill_id
    and bills.leg_id = authors_agg.leg_id
left join introduced_versions
    on bills.bill_id = introduced_versions.bill_id
    and bills.leg_id = introduced_versions.leg_id
left join most_recent_versions
    on bills.bill_id = most_recent_versions.bill_id
    and bills.leg_id = most_recent_versions.leg_id
left join companions_agg
    on bills.bill_id = companions_agg.bill_id
    and bills.leg_id = companions_agg.leg_id
left join most_recent_companion
    on bills.bill_id = most_recent_companion.bill_id
    and bills.leg_id = most_recent_companion.leg_id
-- left join actions
--     on bills.bill_id = actions.bill_id
--     and bills.leg_id = actions.leg_id
order by cast(SUBSTRING(bills.bill_id, 3) as INTEGER)