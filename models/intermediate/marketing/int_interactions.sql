{{ config(
    materialized='incremental',
    unique_key='message_id',
    incremental_strategy='merge'
) }}

WITH interactions AS (
    SELECT
        message_id,             -- PK
        event_time,
        contact_id,             -- FK
        campaign_id,            -- to FK
        campaign_source,        -- to FK
        section_id,
        relative_link_id,       -- to FK
        link_url,               -- to FK
        link_name
    FROM {{ ref('stg_marketing') }}
    -- we have a new contact without event_time
    WHERE event_time IS NOT NULL

    {% if is_incremental() %}
        -- Load only new records when running incrementally
        -- new registration with null event_time will be excluded
        AND event_time > (SELECT MAX(event_time) FROM {{ this }})
    {% endif %}
)

SELECT
    {{ dbt_utils.surrogate_key(['message_id', 'event_time']) }} AS s_message_id,
    message_id,
    contact_id,
    {{ dbt_utils.surrogate_key(['campaign_id', 'campaign_source']) }} AS s_campaign_id,
    campaign_id,
    campaign_source,
    section_id,
    {{ dbt_utils.surrogate_key(['link_url', 'relative_link_id']) }} AS s_link_id,
    link_url,
    relative_link_id,
    link_name,
    event_time
FROM interactions
