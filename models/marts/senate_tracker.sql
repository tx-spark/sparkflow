with tracker as (
    select * from {{ source('bills', 'tracker') }}
)

select * from tracker
where left(bill_id, 2) = 'SB'
