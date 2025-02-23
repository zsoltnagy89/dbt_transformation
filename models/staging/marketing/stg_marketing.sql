-- in the fututre the input will be bigger
-- incremental materialization due to scalability
-- I use a composite unique_key because the duplicated rows
-- contact_id, message_id and event_time are available almost everywhere
-- except 1 row but that is filtered out due to lack of event_time
{{
    config(
        materialized='incremental',
        unique_key=['contact_id', 'message_id', 'event_time'],
        incremental_strategy='merge'
    )
}}

SELECT
    TRY_CAST(contact_id AS INTEGER) AS contact_id,
    country_or_region,
    TRY_TO_DATE(date_of_first_registration, 'YYYY-MM-DD') AS date_of_first_registration,
    TRY_TO_DATE(date_of_last_unsubscription, 'YYYY-MM-DD') AS date_of_last_unsubscription,
    email,
    first_name,
    location,
    TRY_CAST(order_id AS INTEGER) AS order_id,
    TRY_CAST(signup_source AS INTEGER) AS signup_source,
    TRY_CAST(signup_source_v2 AS INTEGER) AS signup_source_v2,
    TRY_CAST(unsub_not_interested_04 AS INTEGER) AS unsub_not_interested_04,
    TRY_CAST(unsub_not_relevant_03 AS INTEGER) AS unsub_not_relevant_03,
    TRY_CAST(unsub_not_remember_signup_01 AS INTEGER) AS unsub_not_remember_signup_01,
    TRY_CAST(unsub_other_05 AS INTEGER) AS unsub_other_05,
    TRY_CAST(unsub_too_many_emails_02 AS INTEGER) AS unsub_too_many_emails_02,
    url,
    TRY_CAST(campaign_id AS INTEGER) AS campaign_id,
    campaign_version_name,
    campaign_category_name,
    source AS campaign_source,
    TRY_CAST(message_id AS INTEGER) AS message_id,
    -- Airbyte ingest everything as VARCHAR
    -- try to cast it but the ' UTC' prefix will block it
    -- so I remove that one
    TRY_TO_TIMESTAMP(REPLACE(event_time, ' UTC', ''), 'YYYY-MM-DD HH24:MI:SS') AS event_time, -- Cast to TIMESTAMP
    TRY_CAST(section_id AS INTEGER) AS section_id,
    TRY_CAST(relative_link_id AS INTEGER) AS relative_link_id,
    link_url,
    link_category_name,
    link_name
FROM {{ source('raw_marketing_airbyte', 'DATASET') }}
-- just keep valid timestamps
WHERE event_time IS NOT NULL AND TRY_TO_TIMESTAMP(REPLACE(event_time, ' UTC', ''), 'YYYY-MM-DD HH24:MI:SS') IS NOT NULL

-- jinja to handle incremental load
{% if is_incremental() %}
-- Only process new rows based on event_time
AND TRY_TO_TIMESTAMP(REPLACE(event_time, ' UTC', ''), 'YYYY-MM-DD HH24:MI:SS') > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}
