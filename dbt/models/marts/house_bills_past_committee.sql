
with tracker as (
    select * from {{ source('bills', 'tracker') }}
),

stages as (
    select * from {{ source('bills','curr_bill_stages')}}
),

most_recent_bill_stage as (
    SELECT * exclude (rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY bill_id, leg_id ORDER BY IFNULL(stage_date, strptime('12/12/9999', '%m/%d/%Y')) DESC) as rn
        FROM stages
    )
    WHERE rn = 1
)

select tracker.*
from tracker
left join most_recent_bill_stage
    on tracker.bill_id = most_recent_bill_stage.bill_id
    and tracker.leg_id = most_recent_bill_stage.leg_id
where most_recent_bill_stage.stage_num >= 2
    and left(tracker.bill_id, 2) = 'HB'