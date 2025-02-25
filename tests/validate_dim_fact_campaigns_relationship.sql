WITH prim_key AS (
    SELECT DISTINCT s_campaign_id
    FROM {{ ref('int_campaigns') }}
)

SELECT DISTINCT s_campaign_id
FROM {{ ref('int_interactions') }}
WHERE s_campaign_id NOT IN (SELECT s_campaign_id FROM prim_key)
    -- explicit case when the 'campaign_id' is null & 'campaign_source' has a value
    AND s_campaign_id != {{ dbt_utils.surrogate_key(['null', "'opens'"]) }}
    AND s_campaign_id != {{ dbt_utils.surrogate_key(['null', "'clicks'"]) }}
