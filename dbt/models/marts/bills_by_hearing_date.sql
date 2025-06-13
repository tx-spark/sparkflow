with committee_meeting_bills as (
    select * from {{ ref('committee_meeting_bills') }}
),

committee_meetings as (
    select * from {{ ref('committee_meetings') }}
),

links as (
    select * from {{ ref('links') }}
),

bills as (
    select * from {{ ref('bills') }}
),

committee_hearing_videos as (
    select * from {{ ref('committee_hearing_videos') }}
)

--------------------------------------

select 
committee_meeting_bills.bill_id,
committee_meeting_bills.leg_id,
bills.caption, -- introduced caption
CONCAT(
    '=HYPERLINK("',
    links.history,
    '", "',
    bills.last_action,
    '")'
) as bill_history,  
CONCAT(
    '=HYPERLINK("',
    committee_meetings.committee_meetings_link,
    '", "',
    committee_meeting_bills.committee_name,
    '")'
) as committee_name, 
committee_meeting_bills.chamber,
CONCAT(
    '=HYPERLINK("',
    committee_meeting_bills.meeting_url,
    '", "',
    FORMAT_TIMESTAMP('%m/%d/%Y %I:%M %p', committee_meeting_bills.meeting_datetime),
    '")'
) as hearing_datetime,
-- committee_meetings.hearing_notice_pdf,
committee_meetings.minutes_pdf,
committee_meetings.witness_list_pdf,
committee_hearing_videos.video_link
--committee_meeting.video_link
from committee_meeting_bills
left join bills 
    on committee_meeting_bills.bill_id = bills.bill_id
    and committee_meeting_bills.leg_id = bills.leg_id
left join committee_meetings
    on committee_meeting_bills.meeting_url = committee_meetings.meeting_url

left join links
    on bills.bill_id = links.bill_id
    and bills.leg_id = links.leg_id

left join 
    committee_hearing_videos
    on 
        array_to_string(regexp_extract_all(lower(committee_meeting_bills.committee_name), '[a-z0-9]+'),'') = array_to_string(regexp_extract_all(lower(committee_hearing_videos.program), '[a-z0-9]+'),'')
        and committee_meeting_bills.leg_id = committee_hearing_videos.leg_id
        and if(left(committee_meeting_bills.bill_id,1) = 'H', 'House', 'Senate') = committee_hearing_videos.chamber
        and (
            (FORMAT_TIMESTAMP('%m/%d/%Y', committee_meeting_bills.meeting_datetime) = committee_hearing_videos.date)
            or (FORMAT_TIMESTAMP('%m/%d/%Y', committee_meeting_bills.meeting_datetime) = committee_hearing_videos.date)
        )
        and (committee_hearing_videos.part = 'I' or committee_hearing_videos.part is null)
order by committee_meeting_bills.meeting_datetime desc,committee_meeting_bills.committee_name, committee_meeting_bills.bill_id
