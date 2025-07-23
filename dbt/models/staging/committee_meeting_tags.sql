select
    `Leg ID` as leg_id,
    `Meeting URL` as meeting_url,
    `Topic Tag` as tag,
    `For | Against | On` as position,
    `Reason` as Reason,
    `Links to orgs and advocates for talking points` as `talking_points`,
    `Note` as Note
from {{ source('raw_bills', 'committee_meeting_tags') }}
group by 1,2,3,4,5,6,7