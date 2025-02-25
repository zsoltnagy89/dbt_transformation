{{ config(
    materialized='table'
) }}

SELECT
    s_campaign_id,
    campaign_id,
    campaign_version_name,
    campaign_category_name,
    campaign_source
FROM {{ ref('int_campaigns') }}
