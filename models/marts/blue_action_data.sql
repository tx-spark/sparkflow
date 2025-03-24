
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

stages as (
    select * from {{ source('bills', 'curr_bill_stages') }}
),

committees as (
    select * from {{ source('bills', 'curr_committees') }}
),

committee_meeting_bills as (
    select * from {{ source('bills', 'curr_committee_meeting_bills') }}
),

committee_hearing_videos as (
    select * from {{ source('bills', 'curr_committee_hearing_videos') }}
),

subjects as (
    select * from {{ source('bills', 'curr_subjects') }}
),

----------------------------------------------------------

authors_agg as (
    SELECT
        bill_id,
        leg_id,
        STRING_AGG(authors.author, ' | ') as authors_list
    FROM
        authors
    where author_type = 'Author'
    GROUP BY
        bill_id,
        leg_id
),

coauthors_agg as (
    SELECT
        bill_id,
        leg_id,
        STRING_AGG(authors.author, ' | ') as authors_list
    FROM
        authors
    where author_type = 'Coauthor'
    GROUP BY
        bill_id,
        leg_id
),

subjects_agg as (
    SELECT
        bill_id,
        leg_id,
        STRING_AGG(subjects.subject_title, ' | ') as subjects
    FROM
        subjects
    GROUP BY
        bill_id,
        leg_id
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
        IF(stage_num = 7 and stages.status = 'Alive', 'Law', stages.status) as bill_status
    from stages
    qualify ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id ORDER BY stage_num DESC) = 1
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

committee_links as (
    select
        leg_id,
        chamber,
        committee_code,
        committee_name,
        committee_meetings_link,
        hearing_notice_pdf,
        minutes_pdf,
        witness_list_pdf
    from committee_meetings
    QUALIFY
        row_number() OVER (PARTITION BY leg_id, chamber, committee_name ORDER BY meeting_datetime DESC) = 1
),

committee_meeting_links as (
    select
        leg_id,
        meeting_datetime,
        chamber,
        committee_name,
        committee_code,
        committee_meetings_link,
        hearing_notice_pdf,
        minutes_pdf,
        witness_list_pdf
    from committee_meetings
),

committees_combine as (
    SELECT
        bill_id,
        committees.leg_id,
        committees.name as  committees_names,
        committee_meeting_links.committee_code as committee_codes,
    FROM
        committees
    -- left join committee_links
    --     on lower(trim(committees.name)) = lower(trim(committee_links.committee_name))
    --     and committees.leg_id = committee_links.leg_id
    --     and committees.chamber = committee_links.chamber
    left join committee_meeting_links
        on committees.leg_id = committee_meeting_links.leg_id
        and committees.chamber = committee_meeting_links.chamber
        and committees.name = committee_meeting_links.committee_name
    where committees.name is not null
    and committees.name != ''
    GROUP BY
        bill_id,
        committees.leg_id,
        3,4
),

committees_agg as (
    select
    bill_id,
    leg_id,
    STRING_AGG(committees_names, ' | ') as committees_names,
    STRING_AGG(committee_codes, ' | ') as committee_codes
    from committees_combine
    group by
        bill_id,
        leg_id
),

committee_meeing_bills_with_links as (
    select
    committee_meeting_bills.*,
    committee_meeting_links.committee_code,
    committee_meeting_links.hearing_notice_pdf,
    committee_meeting_links.minutes_pdf,
    committee_meeting_links.witness_list_pdf,
    committee_hearing_videos.video_link
    from
        committee_meeting_bills
    left join 
        committee_hearing_videos
        on 
            array_to_string(regexp_extract_all(lower(committee_meeting_bills.committee_name), '[a-z0-9]+'),'') = array_to_string(regexp_extract_all(lower(committee_hearing_videos.program), '[a-z0-9]+'),'')
            and committee_meeting_bills.leg_id = committee_hearing_videos.leg_id
            and if(left(committee_meeting_bills.bill_id,1) = 'H', 'House', 'Senate') = committee_hearing_videos.chamber
            and (
                strftime(committee_meeting_bills.meeting_datetime, '%-m/%d/%y') = committee_hearing_videos.date
                or strftime(committee_meeting_bills.meeting_datetime, '%m/%d/%Y') = committee_hearing_videos.date
            )
            and (committee_hearing_videos.part = 'I' or committee_hearing_videos.part is null)
    left join 
        committee_meeting_links
        on 
            committee_meeting_bills.leg_id = committee_meeting_links.leg_id
            and committee_meeting_bills.chamber = committee_meeting_links.chamber
            and committee_meeting_bills.committee_name = committee_meeting_links.committee_name
            and committee_meeting_bills.meeting_datetime = committee_meeting_links.meeting_datetime
),

next_committee_meeting_bills as (
    select 
        bill_id, 
        leg_id,
        committee_name,
        CONCAT(
            '=HYPERLINK("',
            meeting_url,
            '", "',
            strftime(meeting_datetime, '%m/%d/%Y %I:%M %p'),
            '")'
        ) as meeting_datetime,
        video_link,
        hearing_notice_pdf,
        minutes_pdf,
        witness_list_pdf
    from committee_meeing_bills_with_links
    -- and meeting_datetime > CURRENT_DATE()
    qualify row_number() over (PARTITION BY bill_id, leg_id ORDER BY meeting_datetime DESC) = 1
)

----------------------------------------------------------

select
    bills.bill_id as "Bill Number",
    bills.leg_id,
    companions_agg.companions_list as "Companions",
    if(left(bills.bill_id,1) = 'H', 'House', 'Senate') as "Chamber",
    committees_agg.committees_names as "Committee Name",
    committees_agg.committee_codes as "Committee Code",
    bills.caption as "Caption",
    subjects_agg.subjects as "Subjects", 
    bills.last_action as "Last Action",
    authors_agg.authors_list as "Authors",
    coauthors_agg.authors_list as "Coauthors",
    '' as Sponsors, -- sponsors_agg.sponsors_list as "Sponsors",
    '' as Cosponsors, -- cosponsors_agg.cosponsors_list as "Cosponsors",
    links.history as "Bill History URL",
    most_recent_versions.pdf_url as "Bill Text PDF URL",
    next_committee_meeting_bills.hearing_notice_pdf as "Hearing Link",
    next_committee_meeting_bills.committee_name as "Hearing Description"

from bills

left join committees_agg
    on bills.bill_id = committees_agg.bill_id
    and bills.leg_id = committees_agg.leg_id

left join companions_agg
    on bills.bill_id = companions_agg.bill_id
    and bills.leg_id = companions_agg.leg_id

left join authors_agg
    on bills.bill_id = authors_agg.bill_id
    and bills.leg_id = authors_agg.leg_id

left join coauthors_agg
    on bills.bill_id = coauthors_agg.bill_id
    and bills.leg_id = coauthors_agg.leg_id
    
left join subjects_agg
    on bills.bill_id = subjects_agg.bill_id
    and bills.leg_id = subjects_agg.leg_id

-- left join sponsors_agg
--     on bills.bill_id = sponsors_agg.bill_id
--     and bills.leg_id = sponsors_agg.leg_id

-- left join cosponsors_agg
--     on bills.bill_id = cosponsors_agg.bill_id
--     and bills.leg_id = cosponsors_agg.leg_id

left join links
    on bills.bill_id = links.bill_id
    and bills.leg_id = links.leg_id

left join most_recent_versions
    on bills.bill_id = most_recent_versions.bill_id
    and bills.leg_id = most_recent_versions.leg_id

left join next_committee_meeting_bills
    on bills.bill_id = next_committee_meeting_bills.bill_id
    and bills.leg_id = next_committee_meeting_bills.leg_id

order by left(bills.bill_id,2), cast(SUBSTRING(bills.bill_id, 3) as INTEGER)

