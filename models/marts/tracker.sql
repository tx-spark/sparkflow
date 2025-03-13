with complete_bills_list as (
    select * from {{ source('bills', 'curr_complete_bills_list') }}
),
bills as (
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

stages as (
    select * from {{ source('bills', 'curr_bill_stages') }}
),

committees as (
    select * from {{ source('bills', 'curr_committees') }}
),

upcoming_committee_meeting_bills as (
    select * from {{ source('bills', 'curr_upcoming_committee_meeting_bills') }}
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
               ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id, type ORDER BY text_order DESC) as rn
        FROM versions
    ) v
    WHERE rn = 1
    and type = 'Bill'
),

most_recent_fiscal_note as (
    SELECT * exclude (rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id, type ORDER BY text_order DESC) as rn
        FROM versions
    ) v
    WHERE rn = 1
    and type = 'Fiscal Note'
),

most_recent_analysis as (
    SELECT * exclude (rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id, type ORDER BY text_order DESC) as rn
        FROM versions
    ) v
    WHERE rn = 1
    and type = 'Analysis'
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
),

bill_status as (
    SELECT 
        bill_id,
        leg_id,
        IF(stage_num = 7 and status = 'Alive', 'Law', status) as status
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id ORDER BY stage_num DESC) as rn
        FROM stages
    ) 
    WHERE rn = 1
),

most_recent_bill_stage as (
    SELECT * exclude (rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id ORDER BY IFNULL(stage_date, strptime('12/12/9999', '%m/%d/%Y')) DESC) as rn
        FROM stages
    )
    WHERE rn = 1
),

committees_agg as (
    SELECT
        bill_id,
        committees.leg_id,
        CONCAT(
            '=HYPERLINK("',
            MAX(committee_meetings.link),
            '", "',
            STRING_AGG(committees.name, ' | '),
            '")'
        ) as committees_link
    FROM
        committees
    left join committee_meetings
        on lower(trim(committees.name)) = lower(trim(committee_meetings.name))
        and committees.leg_id = committee_meetings.leg_id
        and committees.chamber = committee_meetings.chamber
    where committees.name is not null
    and committees.name != ''
    GROUP BY
        bill_id,
        committees.leg_id
),

upcoming_house_committee_meetings as (
    select 
        bill_id, 
        leg_id, 
        CONCAT(
            '=HYPERLINK("',
            meeting_url,
            '", "',
            strftime(meeting_datetime, '%m/%d/%Y %I:%M %p'),
            '")'
        ) as meeting_datetime
    from upcoming_committee_meeting_bills
    where chamber = 'House'
    qualify row_number() over (PARTITION BY bill_id, leg_id ORDER BY meeting_datetime) = 1
),

upcoming_senate_committee_meetings as (
    select 
        bill_id, 
        leg_id, 
        CONCAT(
            '=HYPERLINK("',
            meeting_url,
            '", "',
            strftime(meeting_datetime, '%m/%d/%Y %I:%M %p'),
            '")'
        ) as meeting_datetime
    from upcoming_committee_meeting_bills
    where chamber = 'Senate'
    qualify row_number() over (PARTITION BY bill_id, leg_id ORDER BY meeting_datetime) = 1
)

----------------------------------------------------------


select
    complete_bills_list.bill_id,
    complete_bills_list.leg_id,
    bills.caption,
    CONCAT(
        '=HYPERLINK("',
        links.history,
        '", "',
        most_recent_bill_stage.stage_title,
        '")'
    ) as bill_history,  
    CONCAT(
        '=HYPERLINK("',
        links.authors,
        '", "',
        REPLACE(authors_agg.authors_list, '"', '""'),
        '")'
    ) as authors,   
    links.captions, -- caption link
    IFNULL(bill_status.status, 'Unassigned') as status, --Bill History/Status
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
    CONCAT(
        '=HYPERLINK("',
        most_recent_versions.pdf_url,
        '", "',
        most_recent_versions.description,
        '")'
    ) as recent_bill_text, -- Recent bill text link
    introduced_versions.pdf_url as introduced_pdf_url, -- Introduced bill text link
    most_recent_fiscal_note.pdf_url as fiscal_note,
    most_recent_analysis.pdf_url as analysis,
    CONCAT(
        '=HYPERLINK("',
        most_recent_companion.companion_history,
        '", "',
        companions_agg.companions_list,
        '")'
    ) as companions,
    links.amendments as amendments,
    links.sponsors as sponsors,
    CONCAT(
        '=HYPERLINK("',
        links.bill_stages,
        '", "',
        most_recent_bill_stage.stage_title,
        '")'
    ) as stages,
    committees_agg.committees_link as committees,
    upcoming_house_committee_meetings.meeting_datetime as upcoming_house_committee_meeting_datetime,
    upcoming_senate_committee_meetings.meeting_datetime as upcoming_senate_committee_meeting_datetime
    
from complete_bills_list -- join on complete bills list so that the list includes Unassigned bills.
left join bills
    on complete_bills_list.bill_id = bills.bill_id
    and complete_bills_list.leg_id = bills.leg_id

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

left join most_recent_fiscal_note
    on bills.bill_id = most_recent_fiscal_note.bill_id
    and bills.leg_id = most_recent_fiscal_note.leg_id

left join most_recent_analysis
    on bills.bill_id = most_recent_analysis.bill_id
    and bills.leg_id = most_recent_analysis.leg_id

left join bill_status
    on bills.bill_id = bill_status.bill_id
    and bills.leg_id = bill_status.leg_id

left join most_recent_bill_stage
    on bills.bill_id = most_recent_bill_stage.bill_id
    and bills.leg_id = most_recent_bill_stage.leg_id

left join committees_agg
    on bills.bill_id = committees_agg.bill_id
    and bills.leg_id = committees_agg.leg_id

left join upcoming_house_committee_meetings
    on bills.bill_id = upcoming_house_committee_meetings.bill_id
    and bills.leg_id = upcoming_house_committee_meetings.leg_id

left join upcoming_senate_committee_meetings
    on bills.bill_id = upcoming_senate_committee_meetings.bill_id
    and bills.leg_id = upcoming_senate_committee_meetings.leg_id

order by cast(SUBSTRING(complete_bills_list.bill_id, 3) as INTEGER)