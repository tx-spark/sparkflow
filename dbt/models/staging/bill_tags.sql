-- select
--     `Leg ID` as leg_id,
--     `Bill ID` as bill_id,
--     `Tag` as tag,
--     `For | Against | On` as position,
--     `Organization` as organization,
--     `Other` as notes
-- from {{ source('raw_bills', 'bill_tags') }}
-- group by 1,2,3,4,5,6

select
    `Leg ID` as leg_id,
    `Bill ID` as bill_id,
    `Topic Tag` as tag,
    `For | Against | On` as position,
    `Reason` as Reason,
    `Links to orgs and advocates for talking points` as `talking_points`,
    `Note` as Note
from {{ source('raw_bills', 'bill_tags') }}
group by 1,2,3,4,5,6,7