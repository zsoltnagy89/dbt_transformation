{{ config(
    materialized='incremental',
    unique_key='contact_id'
) }}

WITH deduplicate AS (
    SELECT
        contact_id,
        date_of_last_unsubscription,
        unsub_not_remember_signup_01,
        unsub_too_many_emails_02,
        unsub_not_relevant_03,
        unsub_not_interested_04,
        unsub_other_05,
        campaign_id
    FROM {{ ref('stg_marketing') }}

    {% if is_incremental() %}
        WHERE date_of_last_unsubscription > (SELECT MAX(date_of_last_unsubscription) FROM {{ this }})
    {% endif %}
    
    QUALIFY ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY date_of_last_unsubscription DESC) = 1
),

sum_up AS (
    SELECT
        contact_id,
        date_of_last_unsubscription,
        COALESCE(unsub_not_remember_signup_01, 0) AS unsub_not_remember_signup_01,
        COALESCE(unsub_too_many_emails_02, 0) AS unsub_too_many_emails_02,
        COALESCE(unsub_not_relevant_03, 0) AS unsub_not_relevant_03,
        COALESCE(unsub_not_interested_04, 0) AS unsub_not_interested_04,
        COALESCE(unsub_other_05, 0) AS unsub_other_05,
        COALESCE(
            COALESCE(unsub_not_remember_signup_01, 0) + COALESCE(unsub_too_many_emails_02, 0) + COALESCE(unsub_not_relevant_03, 0) + COALESCE(unsub_not_interested_04, 0) + COALESCE(unsub_other_05, 0),
            0
        ) AS total_unsub_reason,
        campaign_id
    FROM deduplicate
)

SELECT
    contact_id,
    date_of_last_unsubscription,
    unsub_not_remember_signup_01,
    unsub_too_many_emails_02,
    unsub_not_relevant_03,
    unsub_not_interested_04,
    unsub_other_05,
    total_unsub_reason,
    campaign_id
FROM sum_up