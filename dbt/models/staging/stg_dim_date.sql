{{ config(materialized='view') }}

SELECT 
    halfMD5(
        toString(date) || '|' || toString(venueId)
    ) AS date_key,
    
    CAST(date AS Date) AS match_date,
    formatDateTime(CAST(date AS DateTime), '%H:%i:%S') AS match_time

FROM {{ source('raw_data', 'bronze_fixtures') }}
GROUP BY 1,2,3
