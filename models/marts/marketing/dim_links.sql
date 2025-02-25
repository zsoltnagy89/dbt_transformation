{{ config(
    materialized='table'
) }}

SELECT
    s_link_id,
    link_url,
    relative_link_id,
    link_name,
    link_category_name
FROM {{ ref('int_links') }}
