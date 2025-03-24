with committee_meeting_bills as (
    select * from {{ source('bills', 'curr_committee_meeting_bills') }}
),

committee_meetings as (
    select * from {{ source('bills', 'curr_committee_meetings') }}
),

bills as (
    select * from {{ source('bills', 'curr_bills') }}
)

--------------------------------------

select 
committee_meeting_bills.bill_id,
committee_meeting_bills.leg_id,
bills.caption, -- introduced caption
-- Bill History/Status
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
    strftime(committee_meeting_bills.meeting_datetime, '%m/%d/%Y %I:%M %p'),
    '")'
) as hearing_datetime,
committee_meetings.hearing_notice_pdf,
committee_meetings.minutes_pdf,
committee_meetings.witness_list_pdf
--committee_meeting.video_link
from committee_meeting_bills
left join bills 
    on committee_meeting_bills.bill_id = bills.bill_id
    and committee_meeting_bills.leg_id = bills.leg_id
left join committee_meetings
    on committee_meeting_bills.meeting_url = committee_meetings.meeting_url
order by committee_meeting_bills.meeting_datetime desc
