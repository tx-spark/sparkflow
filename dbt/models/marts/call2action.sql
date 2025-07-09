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

rep_sen_contact_sheet as (
    select * from {{ ref('rep_sen_contact_sheet') }}
),

----------------------------------------------------------

bill_party as (
    select 
    authors.bill_id, 
    authors.leg_id,
    avg(if(rep_sen_contact_sheet.Party = 'D', 1,0)) as p_dem
    from authors
    left join rep_sen_contact_sheet
        on left(authors.bill_id,1) = left(rep_sen_contact_sheet.district_type,1)
        and authors.leg_id = rep_sen_contact_sheet.leg_id
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
)


----------------------------------------------------------

select
    bills.leg_id, -- removed later
    FORMAT_TIMESTAMP('%m/%d/%Y %I:%M %p', committee_meeting_bills.meeting_datetime) as meeting_datetime,
    CONCAT(committee_meeting_bills.chamber, '\n',
    committee_meeting_bills.committee_name, '\n',
    FORMAT_TIMESTAMP('%I:%M %p', committee_meeting_bills.meeting_datetime), '\n',
    committee_meetings.location) as CMT,
    committee_meeting_bills.bill_id,
    bills.caption,
    '' as Position,
    bill_party.p_dem,
    authors_agg.authors_list,
    '' as Reason,
    '' as `Link to orgs and advocates for talking points`,
    '' as Notes,
    links.history,
    IF(
    committee_meetings.chamber = 'Senate',
    "Senate does not allow online public comments",
    concat('https://comments.house.texas.gov/home?c=',committee_meetings.committee_code)) as `Public Comment Link`,
    committee_meeting_bills.meeting_url as `Hearing Link`, 
    cast(regexp_replace(complete_bills_list.bill_id, '[^0-9]+', '') as INTEGER) as `Bill Number`
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
order by committee_meeting_bills.meeting_datetime desc,committee_meeting_bills.committee_name, committee_meeting_bills.bill_id