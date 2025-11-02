{{ config(materialized='table') }}

-- Aggregate the two closest weather records per match
-- into a single averaged row

SELECT
    -- Deterministic unique key for this match’s weather
    halfMD5(
        toString(event_id) || '|' || toString(toStartOfHour(any(match_datetime)))
    ) AS weather_key,

    CAST(event_id AS Int32) AS event_id,
    any(latitude)   AS latitude,
    any(longitude)  AS longitude,

    -- keep the earliest and latest weather timestamps used
    min(timestamp_hour) AS first_weather_hour,
    max(timestamp_hour) AS second_weather_hour,

    -- averaged metrics across the two records
    round(avg(temperature), 3)    AS avg_temperature,
    round(avg(precipitation), 3)  AS avg_precipitation,
    round(avg(rainfall), 3)       AS avg_rainfall,
    round(avg(snowfall), 3)       AS avg_snowfall,
    round(avg(wind_speed), 3)     AS avg_wind_speed

FROM {{ ref('stg_dim_weather') }}
GROUP BY event_id