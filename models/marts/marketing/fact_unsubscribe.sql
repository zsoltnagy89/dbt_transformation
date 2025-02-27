{{ config(
    materialized='incremental',
    unique_key='contact_id'
    ) }}

WITH unsubs AS (
    SELECT
        contact_id,
        date_of_last_unsubscription,
        campaign_id,
        campaign_source,
        {{ dbt_utils.surrogate_key(['campaign_id', 'campaign_source']) }} AS s_campaign_id,
        unsub_not_interested_04 AS not_interested,
        unsub_not_relevant_03 AS not_relevant,
        unsub_not_remember_signup_01 AS not_remember,
        unsub_other_05 AS unsub_other,
        unsub_too_many_emails_02 AS too_many_emails,
        total_unsub_reason
    FROM {{ ref('int_unsubscribe') }}

    {% if is_incremental() %}
        WHERE date_of_last_unsubscription > (SELECT MAX(date_of_last_unsubscription) FROM {{ this }})
    {% endif %}
)

SELECT 
    contact_id,
    s_campaign_id,
    date_of_last_unsubscription,
    SUM(not_interested) OVER (PARTITION BY s_campaign_id) AS total_not_interested,
    SUM(too_many_emails) OVER (PARTITION BY s_campaign_id) AS total_too_many_emails,
    COUNT(contact_id) OVER (PARTITION BY s_campaign_id) AS total_unsubscriptions,
    ROUND(100 * total_unsubscriptions / NULLIF((SELECT COUNT(DISTINCT contact_id) FROM {{ ref('int_unsubscribe') }}), 0), 2) AS unsubscribe_rate
FROM unsubs
