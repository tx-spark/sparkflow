new_bills AS (
    SELECT * FROM {{ source('new', 'stg_bills') }}
),

existing_bills AS (
    SELECT * FROM {{ source('bills', 'bills') }}
),

new_only_bills AS (
    -- New bills that don't exist in current bills table
    SELECT
        nb.*,
        date_trunc('minute', current_timestamp) as first_seen_at,
        date_trunc('minute', current_timestamp) as last_seen_at
    FROM new_bills nb
    LEFT JOIN existing_bills eb ON nb.bill_id = eb.bill_id AND nb.leg_id = eb.leg_id AND nb.caption = eb.caption AND nb.last_action_date = eb.last_action_date AND nb.last_action_chamber = eb.last_action_chamber AND nb.last_action = eb.last_action AND nb.caption_version = eb.caption_version
    WHERE eb.bill_id IS NULL
),

same_bills AS (
    -- Existing bills that are still present
    SELECT 
        nb.*
        eb.first_seen_at,
        date_trunc('minute', current_timestamp) as last_seen_at
    FROM new_bills nb
    INNER JOIN existing_bills eb ON nb.bill_id = eb.bill_id AND nb.leg_id = eb.leg_id AND nb.caption = eb.caption AND nb.last_action_date = eb.last_action_date AND nb.last_action_chamber = eb.last_action_chamber AND nb.last_action = eb.last_action AND nb.caption_version = eb.caption_version
),

removed_bills AS (
    -- Old bills that are no longer present
    SELECT
        eb.*
    FROM existing_bills eb
    LEFT JOIN new_bills nb ON eb.bill_id = nb.bill_id AND eb.leg_id = nb.leg_id AND eb.caption = nb.caption AND eb.last_action_date = nb.last_action_date AND eb.last_action_chamber = nb.last_action_chamber AND eb.last_action = nb.last_action AND eb.caption_version = nb.caption_version
    WHERE nb.bill_id IS NULL
)

SELECT * FROM new_only_bills
UNION ALL
SELECT * FROM same_bills
UNION ALL 
SELECT * FROM removed_bills
