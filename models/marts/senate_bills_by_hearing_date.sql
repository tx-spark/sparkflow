with bills_by_hearing_date as (
    select * from {{ source('bills','bills_by_hearing_date')}}
)

select * from bills_by_hearing_date
where left(bill_id, 2) = 'SB'