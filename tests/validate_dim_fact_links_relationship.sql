WITH prim_key AS (
    SELECT DISTINCT s_link_id
    FROM {{ ref('int_links') }}
)

SELECT DISTINCT s_link_id
FROM {{ ref('int_interactions') }}
WHERE s_link_id NOT IN (SELECT s_link_id FROM prim_key)
    -- explicit case when the 'link_url' & 'relative_link_id' are null
    AND s_link_id != {{ dbt_utils.surrogate_key(['null', 'null']) }}
