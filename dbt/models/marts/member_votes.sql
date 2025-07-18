with bills as (
    select * from {{source('raw_bills', 'legiscan_bills')}}
),
votes as (
    select * from {{source('raw_bills','legiscan_votes')}}
),
people as (
    select * from {{source('raw_bills', 'legiscan_people')}}
)

select 
  bill_number as bill_id,
  replace(session_name, 'th Legislature Regular Session','R') as leg_id,
  votes.date,
  votes.vote_text as vote,
  name,
  district,
  concat('https://ballotpedia.org/',ballotpedia) as ballotpedia_link
from  bills
left join votes 
    using(legiscan_bill_id)
left join people
    on votes.legiscan_people_id = people.people_id
where votes.date is not null
    and (
        `desc` like 'Read 3rd time%' 
        or `desc` like 'Failed to pass to 3rd reading%'
    )
order by 
  regexp_extract(bill_number, '[A-Za-z]+'),
  cast(regexp_extract(bill_number, '[0-9]+') as INTEGER),
  replace(session_name, 'th Legislature Regular Session','R'),
  votes.vote_text desc