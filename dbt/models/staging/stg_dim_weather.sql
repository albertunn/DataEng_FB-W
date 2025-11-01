-- models/dimensions/dim_weather.sql 

{{ config(materialized='view') }}

SELECT
    halfMD5( -- unique key based on the match and the start of the hour. this is crucial because the raw data has multiple rows per match (24 hourly weather records per one match)
        toString(eventId) || '|' || toString(toStartOfHour(timestamp_utc))
    ) AS weather_key,
    
    eventId AS event_id,
    
    -- AGGREGATION: we use 'any' for latitude and longitude because 
    -- they should be constant within the same eventId and hour.
    any(latitude) AS latitude,       
    any(longitude) AS longitude,    
    
    -- GROUPING FIELD: Defines the group level (the start of the hour).
    toStartOfHour(timestamp_utc) AS timestamp_hour,
    
    -- We use max, avg for grouping purposes
    MAX(date) AS weather_date,     
    formatDateTime(toStartOfHour(timestamp_utc), '%H:%M:%S') AS weather_time,

    round(AVG(temperature_2m), 3) AS temperature, 
    round(AVG(precipitation), 3) AS precipitation,
    round(AVG(rain), 3) AS rainfall,
    round(AVG(snowfall), 3) AS snowfall,
    round(AVG(wind_speed_10m), 3) AS wind_speed

FROM {{ source('raw_data', 'bronze_weather') }}

GROUP BY 
    eventId, 
    toStartOfHour(timestamp_utc) 