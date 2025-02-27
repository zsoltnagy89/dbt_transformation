{{ config(
    materialized='incremental',
    unique_key='contact_id'
) }}

WITH contacts AS (
    SELECT 
        contact_id,
        COALESCE(country_or_region, 'Unknown') AS country_or_region,
        date_of_first_registration,
        date_of_last_unsubscription,
        email,
        first_name,
        COALESCE(location, 'Unknown') AS location,
        signup_source,
        signup_source_v2,
        ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY date_of_first_registration ASC) AS row_num
    FROM {{ ref('stg_marketing') }}

    {% if is_incremental() %}
    WHERE date_of_first_registration >= (SELECT MAX(date_of_first_registration) FROM {{ this }})
    {% endif %}
)

SELECT
    contact_id,     -- PK
    country_or_region,
    date_of_first_registration,
    date_of_last_unsubscription,
    email,
    first_name,
    location,
    signup_source,
    signup_source_v2,
FROM contacts
-- keep the first regtistration date
WHERE row_num = 1
