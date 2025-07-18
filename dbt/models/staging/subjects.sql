with tlo_topics_crosswalk as (
    SELECT
  GSI,
  `tx_spark Categories`,

  array_concat(
    REGEXP_EXTRACT_ALL(
      `tx_spark Categories`,
      r'"([^"]+)"'
    ),
  SPLIT(TRIM(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
      `tx_spark Categories`,
      r'(^\s*"[^"]+"\s*,?\s*)',  -- remove leading quoted value(s)
      ','
    ),
    r'(,\s*"[^"]+"\s*)',         -- remove remaining quoted values
    ''
    ), ', '
  ),', ')
  ) AS txspark_topics_array

FROM
    {{source('raw_bills', 'tlo2txspark_topics_crosswalk')}}
),

subjects as (
    SELECT
        bill_id,
        leg_id,
        subject_title,
        subject_id,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  first_seen_at) as first_seen_at,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',  last_seen_at) as last_seen_at
    FROM {{ source('raw_bills', 'subjects') }}
),

most_recent_timestamp as ( -- I'm assuming that there are no bills without subjects
    select max(last_seen_at) as seen_at_timestamp
    from subjects
),

-- remove all the subjects that have dropped off
current_subjects AS (
    SELECT
        *
    FROM subjects
    inner join most_recent_timestamp
        on subjects.last_seen_at = most_recent_timestamp.seen_at_timestamp 
)

select
    bill_id,
    leg_id,
    subject_title,
    subject_id,
    tlo_topics_crosswalk.txspark_topics_array,
    first_seen_at,
    last_seen_at
from current_subjects
left join tlo_topics_crosswalk
    on  UPPER(coalesce(REGEXP_EXTRACT(subject_title, r'^(.*?)\s*--'), subject_title)) = UPPER(tlo_topics_crosswalk.GSI)