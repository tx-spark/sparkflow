select
    leg_id,
    bill_id,
    count(*) as num_rows
from {{ source('bills', 'tracker') }}
group by 1, 2
having num_rows > 1