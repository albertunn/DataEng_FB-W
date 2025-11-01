{{ config(materialized='view') }}

SELECT 
    halfMD5(toString(venueId)) AS venue_key,
    CAST(venueId AS Int32) AS venue_id,
    NULLIF(TRIM(fullName), '') AS venue_name,
    NULLIF(TRIM(city), '') AS city,
    NULLIF(TRIM(country), '') AS country

FROM {{ source('raw_data', 'bronze_venues') }}