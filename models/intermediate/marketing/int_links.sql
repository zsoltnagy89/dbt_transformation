{{ config(
    materialized='table'
) }}

WITH combination AS (
    SELECT DISTINCT
        link_url,
        relative_link_id,
        link_name,
        link_category_name
    FROM {{ ref('stg_marketing') }}
    WHERE link_url IS NOT NULL
        AND relative_link_id  IS NOT NULL
)

SELECT
    {{ dbt_utils.surrogate_key(['link_url', 'relative_link_id']) }} AS s_link_id,
    *
FROM combination
