select
    leg_id,
    left(bill_id, 2) as chamber,
    count(*) as num_rows,
    max(cast(REGEXP_EXTRACT(bill_id, '[0-9]+') as INTEGER)) as max_bill_id,
    min(cast(REGEXP_EXTRACT(bill_id, '[0-9]+') as INTEGER)) as min_bill_id
from {{ source('bills', 'tracker') }}
group by 1, 2
having num_rows != max_bill_id
or 1 != min_bill_id