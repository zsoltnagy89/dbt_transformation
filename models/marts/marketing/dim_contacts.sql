{{ config(
    materialized='incremental',
    unique_key='contact_id'
) }}

SELECT
    contact_id,
    country_or_region,
    date_of_first_registration,
    date_of_last_unsubscription,
    email,
    first_name,
    location,
    signup_source,
    signup_source_v2
FROM {{ ref('int_contacts') }}

{% if is_incremental() %}
WHERE date_of_first_registration >= (SELECT MAX(date_of_first_registration) FROM {{ this }})
{% endif %}
