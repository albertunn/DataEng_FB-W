{{ config(materialized='view') }}

SELECT
    halfMD5(
        toString(latitude) || '|' || toString(longitude) || '|' || toString(timestamp_utc)
    ) AS weather_key,
    
    latitude as latitude, 
    longitude as longitude,

    timestamp_utc,
    date as weather_date,
    formatDateTime(timestamp_utc, '%H:%M:%S') AS weather_time, 
    
    round(temperature_2m, 3) AS temperature,
    round(precipitation, 3) AS precipitation,
    round(rain, 3) AS rainfall,
    round(snowfall, 3) AS snowfall,
    round(wind_speed_10m, 3) AS wind_speed

FROM {{ source('raw_data', 'bronze_weather') }}

