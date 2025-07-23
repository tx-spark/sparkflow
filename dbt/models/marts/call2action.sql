with bills as (
    select * from {{ ref('bills') }}
),

committee_meeting_bills as (
    select * from {{ ref('committee_meeting_bills') }}
),

committee_meetings as (
    select * from {{ ref('committee_meetings') }}
),

authors as (
    select * from {{ ref('authors') }}
),

links as (
    select * from {{ ref('links') }}
),

subjects as (
    select * from {{ ref('subjects') }}
),

rep_sen_contact_sheet as (
    select * from {{ ref('rep_sen_contact_sheet') }}
),

committee_meeting_tags as (
  select * from {{ ref('committee_meeting_tags')}}
),

----------------------------------------------------------

bill_party as (
    select 
    authors.bill_id, 
    authors.leg_id,
    concat(safe_cast(avg(if(rep_sen_contact_sheet.Party = 'D', 1,0)) * 100 as STRING),'') as p_dem
    from authors
    left join rep_sen_contact_sheet
        on left(authors.bill_id,1) = left(rep_sen_contact_sheet.district_type,1)
        and left(authors.leg_id, 2) = left(rep_sen_contact_sheet.leg_id, 2) -- removes the trailing character indicating regular or special session
        and authors.author = rep_sen_contact_sheet.author_id
    group by 1,2
),

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

bill_tags_intermediate as (
  select
  bill_id,
  leg_id,
  ARRAY_AGG(position) as positions,
  ARRAY_AGG(tag) AS tags,
  ARRAY_AGG(talking_points) AS talking_points,
  ARRAY_AGG(reason) AS reasons,
  ARRAY_AGG(note) AS notes
  from `txspark.tx_leg_bills.bill_tags`
  group by 1,2
),

bill_tags_agg as (
    select
    bill_id,
    leg_id,
    CASE UPPER(ARRAY_TO_STRING(
        ARRAY(
            SELECT 
            DISTINCT position
            FROM UNNEST(positions) as position
            order by position
        ), ', '))
    WHEN 'AGAINST' THEN 'Against'
    WHEN 'FOR' THEN 'For'
    WHEN 'AGAINST, FOR' THEN 'Conflicted'
    WHEN 'AGAINST, ON' THEN 'Against'
    WHEN 'FOR, ON' THEN 'For'
    WHEN 'AGAINST, FOR, ON' THEN 'Conflicted'
    ELSE NULL END as position,

    ARRAY_TO_STRING(
        ARRAY(
        SELECT DISTINCT talking_point
        FROM UNNEST(talking_points) AS talking_point
        ), '\n\n'
    ) AS talking_points,

    ARRAY_TO_STRING(
        ARRAY(
        SELECT DISTINCT reason
        FROM UNNEST(reasons) AS reason
        ), '\n\n'
    ) AS reason,

    ARRAY_TO_STRING(
        ARRAY(
        SELECT DISTINCT note
        FROM UNNEST(notes) AS note
        ), '\n\n'
    ) AS notes
    from bill_tags_intermediate
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
),

important_meetings as (
  select 
    committee_meeting_tags.leg_id,
    FORMAT_TIMESTAMP('%m/%d/%Y %I:%M %p', committee_meetings.meeting_datetime) as meeting_datetime,
    CONCAT(committee_meetings.chamber, '\n',
            committee_meetings.committee_name, '\n',
            FORMAT_TIMESTAMP('%I:%M %p', committee_meetings.meeting_datetime), '\n',
            committee_meetings.location
    ) as `CMT`,
    "Important Meeting" as bill_id,
    committee_meeting_tags.tag as Topics,
    '' as Caption,
    committee_meeting_tags.position as Position,
    committee_meeting_tags.Reason,
    committee_meeting_tags.talking_points as `Link to orgs and advocates for talking points`,
    committee_meeting_tags.note as Notes,
    '' as history,
      IF(
        committee_meetings.chamber = 'Senate',
        "Senate does not allow online public comments",
      concat('https://comments.house.texas.gov/home?c=',committee_meetings.committee_code)) as `Public Comment Link`,
    committee_meetings.meeting_url as `Hearing Link`,
    NULL as `Bill Number` 
  from committee_meetings
  inner join committee_meeting_tags using (meeting_url)
)
----------------------------------------------------------

select
    bills.leg_id, -- removed later
    FORMAT_TIMESTAMP('%m/%d/%Y %I:%M %p', committee_meeting_bills.meeting_datetime) as meeting_datetime,
    CONCAT(committee_meeting_bills.chamber, '\n',
        committee_meeting_bills.committee_name, '\n',
        FORMAT_TIMESTAMP('%I:%M %p', committee_meeting_bills.meeting_datetime), '\n',
        committee_meetings.location
    ) as `CMT`,
    committee_meeting_bills.bill_id,
    txspark_topics.unique_topics as `topics`,
    bills.caption,
    bill_tags_agg.position as Position,
    -- bill_party.p_dem,
    -- authors_agg.authors_list,
    bill_tags_agg.Reason as Reason,
    bill_tags_agg.talking_points as `Link to orgs and advocates for talking points`,
    bill_tags_agg.notes as Notes,
    links.history,
    IF(
    committee_meetings.chamber = 'Senate',
    "Senate does not allow online public comments",
    concat('https://comments.house.texas.gov/home?c=',committee_meetings.committee_code)) as `Public Comment Link`,
    committee_meeting_bills.meeting_url as `Hearing Link`, 
    cast(regexp_replace(committee_meeting_bills.bill_id, '[^0-9]+', '') as INTEGER) as `Bill Number`

from committee_meeting_bills
left join bill_party 
  on committee_meeting_bills.bill_id = bill_party.bill_id 
  and committee_meeting_bills.leg_id = bill_party.leg_id 

left join bills
  on committee_meeting_bills.bill_id = bills.bill_id 
  and committee_meeting_bills.leg_id = bills.leg_id 

left join committee_meetings
  on committee_meeting_bills.meeting_url = committee_meetings.meeting_url

left join links
  on committee_meeting_bills.bill_id = links.bill_id 
  and committee_meeting_bills.leg_id = links.leg_id 

left join authors_agg
  on committee_meeting_bills.bill_id = authors_agg.bill_id 
  and committee_meeting_bills.leg_id = authors_agg.leg_id

left join bill_tags_agg
  on committee_meeting_bills.bill_id = bill_tags_agg.bill_id 
  and committee_meeting_bills.leg_id = bill_tags_agg.leg_id

left join txspark_topics
    on committee_meeting_bills.bill_id = txspark_topics.bill_id
    and committee_meeting_bills.leg_id = txspark_topics.leg_id

UNION ALL

select * from important_meetings
    
order by 1 desc, 2, 3