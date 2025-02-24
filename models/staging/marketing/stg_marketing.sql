-- in the fututre the input will be bigger
-- incremental materialization should be better due to scalability
-- I need a unique id to every row in Google sheet
{{ config(
    materialized='incremental',
    unique_key='sk_id'
) }}

SELECT
    {{ dbt_utils.surrogate_key(['contact_id', 'order_id', 'campaign_id', 'message_id', 'section_id', 'relative_link_id']) }} as sk_id,
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
    link_name,
    TRY_TO_TIMESTAMP(LEFT(_airbyte_extracted_at, 19), 'YYYY-MM-DD HH24:MI:SS') as ingestion_time
FROM {{ source('raw_marketing_airbyte', 'DATASET') }}

-- jinja to handle incremental load
{% if is_incremental() %}
-- Only process new rows based on event_time
WHERE TRY_TO_TIMESTAMP(REPLACE(event_time, ' UTC', ''), 'YYYY-MM-DD HH24:MI:SS') > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}

-- there are some edge cases whene the sk_id is not unique
-- It could be messages, which were updated by the user
-- I just keep the latest event from the first load
-- in the future the merge incremental strategy (insert?update) should be able to solve it
QUALIFY ROW_NUMBER() OVER (PARTITION BY sk_id ORDER BY event_time DESC) = 1
