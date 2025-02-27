{{ config(
    materialized='incremental',
    unique_key='s_message_id'
    ) }}

WITH interactions AS (
    SELECT 
        s_message_id,
        message_id,
        contact_id,
        s_campaign_id,
        campaign_id,
        campaign_source,
        section_id,
        s_link_id,
        link_url,
        relative_link_id,
        link_name,
        event_time,
        -- KPI Metrics
        CASE WHEN event_time IS NOT NULL THEN 1 ELSE 0 END AS sent_email,
        CASE WHEN link_url IS NOT NULL THEN 1 ELSE 0 END AS clicked_email
    FROM {{ ref('int_interactions') }}

    {% if is_incremental() %}
    WHERE event_time >= (SELECT MAX(event_time) FROM {{ this }})
    {% endif %}
)

SELECT 
    s_message_id,
    message_id,
    contact_id,
    s_campaign_id,
    section_id,
    s_link_id,
    event_time,
    sent_email,
    clicked_email,
    SUM(sent_email) OVER (PARTITION BY s_campaign_id) AS total_sent_emails,
    SUM(clicked_email) OVER (PARTITION BY s_campaign_id) AS total_clicks,
    ROUND(100 * total_clicks / NULLIF(total_sent_emails, 0), 2) AS click_through_rate
FROM interactions
