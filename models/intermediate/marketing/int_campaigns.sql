-- reconstruct the table from scratch
-- due to the several possible combiantion
{{ config(
    materialized='table'
) }}

WITH camp_ids AS (
    SELECT DISTINCT campaign_id
    FROM {{ ref('stg_marketing') }}
    WHERE campaign_id IS NOT NULL
),

camp_vers_name AS (
    SELECT DISTINCT
        campaign_id,
        campaign_version_name
    FROM  {{ ref('stg_marketing') }}
    WHERE campaign_version_name IS NOT NULL
),

camp_cat_name AS (
    SELECT DISTINCT
        campaign_id,
        campaign_category_name
    FROM  {{ ref('stg_marketing') }}
    WHERE campaign_category_name IS NOT NULL
),

c_source AS (
    SELECT DISTINCT
        campaign_id,
        campaign_source
    FROM  {{ ref('stg_marketing') }}
    WHERE campaign_source IS NOT NULL
),

merged AS(
SELECT
    id.campaign_id,
    vs_name.campaign_version_name,
    cat_name.campaign_category_name,
    c_source.campaign_source
FROM camp_ids AS id
LEFT JOIN camp_vers_name as vs_name USING (campaign_id)
LEFT JOIN camp_cat_name as cat_name USING (campaign_id)
LEFT JOIN c_source USING (campaign_id)
)

-- we have campaign_ids with the same campaign_source --> use surrogate_key
SELECT
    {{ dbt_utils.surrogate_key(['campaign_id', 'campaign_source']) }} AS s_campaign_id,
    campaign_id,
    campaign_version_name,
    campaign_category_name,
    campaign_source
FROM merged