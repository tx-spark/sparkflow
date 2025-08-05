with complete_bills_list as (
    select * from {{ ref('complete_bills_list') }}
),

bills as (
    select * from {{ ref('bills') }}
),

links as (
    select * from {{ ref('links') }}
),

authors as (
    select * from {{ ref('authors') }}
),

actions as (
    select * from {{ ref('actions') }}
),

versions as (
    select * from {{ ref('versions') }}
),

companions as (
    select * from {{ ref('companions') }}
),

committee_meetings as (
    select * from {{ ref('committee_meetings') }}
),

stages as (
    select * from {{ ref('bill_stages') }}
),

committees as (
    select * from {{ ref('committee_status') }}
),

committee_meeting_bills as (
    select * from {{ ref('committee_meeting_bills') }}
),

committee_hearing_videos as (
    select * from {{ ref('committee_hearing_videos') }}
),

rep_sen_contact_sheet as (
    select * from {{ ref('rep_sen_contact_sheet') }}
),

bill_tags as (
    select * from {{ ref('bill_tags') }}
),

subjects as (
    select * from {{ ref('subjects') }}
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
    SELECT * EXCEPT (rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id, type ORDER BY text_order DESC) as rn
        FROM versions
    ) v
    WHERE rn = 1
    and type = 'Bill'
),

most_recent_fiscal_note as (
    SELECT * EXCEPT (rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id, type ORDER BY text_order DESC) as rn
        FROM versions
    ) v
    WHERE rn = 1
    and type = 'Fiscal Note'
),

most_recent_analysis as (
    SELECT * EXCEPT (rn)
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
    SELECT c.* EXCEPT (rn), 
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
        stages.bill_id,
        stages.leg_id,
        CASE 
            WHEN bills.last_action like '%Failed to receive affirmative vote in comm%' THEN 'Dead'
            WHEN bills.last_action like '%Vetoed%' THEN 'Vetoed'
            WHEN stage_num = 7 and stages.status = 'Alive' THEN 'Law'
            ELSE stages.status 
        END as bill_status
    from stages 
    left join bills
        on stages.bill_id = bills.bill_id
        and stages.leg_id = bills.leg_id
    qualify ROW_NUMBER() OVER (PARTITION BY stages.bill_id, stages.leg_id ORDER BY stage_num DESC) = 1
),

most_recent_bill_stage as (
    SELECT * EXCEPT (rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id ORDER BY IFNULL(stage_date, PARSE_TIMESTAMP('%m/%d/%Y', '12/12/9999')) DESC) as rn
        FROM stages
    )
    WHERE rn = 1
),

committee_links as (
    select
        leg_id,
        chamber,
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
        committee_meetings_link,
        hearing_notice_pdf,
        minutes_pdf,
        witness_list_pdf
    from committee_meetings
),

committees_agg as (
    SELECT
        bill_id,
        committees.leg_id,
        CONCAT(
            '=HYPERLINK("',
            ANY_VALUE(committee_links.committee_meetings_link),
            '", "',
            STRING_AGG(committees.name, ' | '),
            '")'
        ) as committees_link
    FROM
        committees
    left join committee_links
        on lower(trim(committees.name)) = lower(trim(committee_links.committee_name))
        and committees.leg_id = committee_links.leg_id
        and committees.chamber = committee_links.chamber
    where committees.name is not null
    and committees.name != ''
    GROUP BY
        bill_id,
        committees.leg_id
),

house_committees_agg as (
    SELECT
        bill_id,
        committees.leg_id,
        CONCAT(
            '=HYPERLINK("',
            ANY_VALUE(committee_links.committee_meetings_link),
            '", "',
            STRING_AGG(committees.name, ' | '),
            '")'
        ) as committees_link
    FROM
        committees
    left join committee_links
        on lower(trim(committees.name)) = lower(trim(committee_links.committee_name))
        and committees.leg_id = committee_links.leg_id
        and committees.chamber = committee_links.chamber
    where committees.name is not null and committees.chamber = 'House'
    and committees.name != ''
    GROUP BY
        bill_id,
        committees.leg_id
),

senate_committees_agg as (
    SELECT
        bill_id,
        committees.leg_id,
        CONCAT(
            '=HYPERLINK("',
            ANY_VALUE(committee_links.committee_meetings_link),
            '", "',
            STRING_AGG(committees.name, ' | '),
            '")'
        ) as committees_link
    FROM
        committees
    left join committee_links
        on lower(trim(committees.name)) = lower(trim(committee_links.committee_name))
        and committees.leg_id = committee_links.leg_id
        and committees.chamber = committee_links.chamber
    where committees.name is not null and committees.chamber = 'Senate'
    and committees.name != ''
    GROUP BY
        bill_id,
        committees.leg_id
),

committee_meeing_bills_with_links as (
    select
    committee_meeting_bills.*,
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
                FORMAT_TIMESTAMP('%m/%d/%Y', committee_meeting_bills.meeting_datetime) = committee_hearing_videos.date
                or FORMAT_TIMESTAMP('%m/%d/%Y', committee_meeting_bills.meeting_datetime) = committee_hearing_videos.date
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

first_house_committee_meeting_bills as (
    select 
        bill_id, 
        leg_id, 
        CONCAT(
            '=HYPERLINK("',
            meeting_url,
            '", "',
            FORMAT_TIMESTAMP('%m/%d/%Y %I:%M %p', meeting_datetime),
            '")'
        ) as meeting_datetime,
        video_link,
        hearing_notice_pdf,
        minutes_pdf,
        witness_list_pdf
    from committee_meeing_bills_with_links
    where chamber = 'House'
    and status != 'deleted'
    -- and meeting_datetime > CURRENT_DATE()
    qualify row_number() over (PARTITION BY bill_id, leg_id ORDER BY meeting_datetime) = 1
),

first_senate_committee_meeting_bills as (
    select 
        bill_id, 
        leg_id, 
        CONCAT(
            '=HYPERLINK("',
            meeting_url,
            '", "',
            FORMAT_TIMESTAMP('%m/%d/%Y %I:%M %p', meeting_datetime),
            '")'
        ) as meeting_datetime,
        video_link,
        hearing_notice_pdf,
        minutes_pdf,
        witness_list_pdf
    from committee_meeing_bills_with_links
    where chamber = 'Senate' 
    and status != 'deleted'
    -- and meeting_datetime > CURRENT_DATE()
    qualify row_number() over (PARTITION BY bill_id, leg_id ORDER BY meeting_datetime) = 1
),

bill_party as (
    select 
    authors.bill_id, 
    authors.leg_id,
    concat(safe_cast(avg(if(rep_sen_contact_sheet.Party = 'D', 1,0)) * 100 as STRING),'') as p_dem
    from authors
    left join rep_sen_contact_sheet
        on left(authors.bill_id,1) = left(rep_sen_contact_sheet.district_type,1)
        and left(authors.leg_id, 2) = left(safe_cast(rep_sen_contact_sheet.leg_id as STRING), 2) -- removes the trailing character indicating regular or special session
        and authors.author = rep_sen_contact_sheet.author_id
    group by 1,2
),

bill_tags_agg as (
    select
    bill_id,
    leg_id,
    MAX(position) as position,
    STRING_AGG(tag, ' | ') as tags
    from bill_tags
    group by 1,2
),

grouped_txspark_topics as (
  select 
    leg_id,
    bill_id, 
    STRING_AGG(subject_title, ' | ') as subjects,
    ARRAY_CONCAT_AGG(txspark_topics_array) as txspark_topics_array
  from subjects
  group by 1,2
),

txspark_topics as (
SELECT 
  leg_id, 
  bill_id, 
  subjects,
  ARRAY_TO_STRING(
    ARRAY(
      SELECT 
        DISTINCT topics
      FROM UNNEST(ARRAY_CONCAT(txspark_topics_array)) AS topics 
      where topics != 'TBD' and topics != ''
    ), ' | ') AS unique_topics
FROM grouped_txspark_topics

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
        bills.last_action,
        '")'
    ) as bill_history,  
    CONCAT(
        '=HYPERLINK("',
        links.authors,
        '", "',
        REPLACE(authors_agg.authors_list, '"', '""'),
        '")'
    ) as authors,
    txspark_topics.unique_topics,
    INITCAP(bill_tags_agg.position) as position,
    bill_party.p_dem,
    links.captions, -- caption link
    IFNULL(bill_status.bill_status, 'Unassigned') as status,
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
    if(most_recent_bill_stage.stage_title is not null, CONCAT(
        '=HYPERLINK("',
        links.bill_stages,
        '", "Stage ',
        most_recent_bill_stage.stage_num,
        ': ',
        most_recent_bill_stage.stage_title,
        '")'
    ), Null) as stages,
    house_committees_agg.committees_link as house_committees,
    first_house_committee_meeting_bills.meeting_datetime as first_house_committee_meeting_datetime,
    first_house_committee_meeting_bills.video_link as first_house_committee_video_link,
    first_house_committee_meeting_bills.witness_list_pdf as first_house_committee_witness_list_pdf,
    -- first_house_committee_meeting_bills.hearing_notice_pdf as first_house_committee_hearing_notice_pdf,
    -- first_house_committee_meeting_bills.minutes_pdf as first_house_committee_minutes_pdf,

    senate_committees_agg.committees_link as senate_committees,
    first_senate_committee_meeting_bills.meeting_datetime as first_senate_committee_meeting_datetime,
    first_senate_committee_meeting_bills.video_link as first_senate_committee_video_link,
    first_senate_committee_meeting_bills.witness_list_pdf as first_senate_committee_witness_list_pdf,
    -- first_senate_committee_meeting_bills.hearing_notice_pdf as first_senate_committee_hearing_notice_pdf,
    -- first_senate_committee_meeting_bills.minutes_pdf as first_senate_committee_minutes_pdf,

    cast(regexp_replace(complete_bills_list.bill_id, '[^0-9]+', '') AS INTEGER) as `#`
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

left join house_committees_agg
    on bills.bill_id = house_committees_agg.bill_id
    and bills.leg_id = house_committees_agg.leg_id

left join senate_committees_agg
    on bills.bill_id = senate_committees_agg.bill_id
    and bills.leg_id = senate_committees_agg.leg_id

left join first_house_committee_meeting_bills
    on bills.bill_id = first_house_committee_meeting_bills.bill_id
    and bills.leg_id = first_house_committee_meeting_bills.leg_id

left join first_senate_committee_meeting_bills
    on bills.bill_id = first_senate_committee_meeting_bills.bill_id
    and bills.leg_id = first_senate_committee_meeting_bills.leg_id

left join bill_party
    on bills.bill_id = bill_party.bill_id
    and bills.leg_id = bill_party.leg_id

left join bill_tags_agg
    on bills.bill_id = bill_tags_agg.bill_id
    and bills.leg_id = bill_tags_agg.leg_id

left join txspark_topics
    on bills.bill_id = txspark_topics.bill_id
    and bills.leg_id = txspark_topics.leg_id

order by cast(regexp_replace(complete_bills_list.bill_id, '[^0-9]+', '') as INTEGER)
