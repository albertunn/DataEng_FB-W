{{ config(materialized='view') }}

-- Keep only two weather records per match:
--  1) the record closest to the match time
--  2) the record immediately after the match time

WITH weather_raw AS (
    SELECT
        CAST(w.eventId AS Int32) AS event_id,
        toDateTime(w.timestamp_utc) AS weather_datetime,
        toStartOfHour(w.timestamp_utc) AS weather_hour,
        CAST(w.latitude AS Float32) AS latitude,
        CAST(w.longitude AS Float32) AS longitude,
        CAST(w.temperature_2m AS Float32) AS temperature,
        CAST(w.precipitation AS Float32) AS precipitation,
        CAST(w.rain AS Float32) AS rainfall,
        CAST(w.snowfall AS Float32) AS snowfall,
        CAST(w.wind_speed_10m AS Float32) AS wind_speed
    FROM {{ source('raw_data', 'bronze_weather') }} AS w
),

fixtures AS (
    SELECT
        CAST(eventId AS Int32) AS event_id,
        toDateTime(date) AS match_datetime
    FROM {{ source('raw_data', 'bronze_fixtures') }}
),

ranked_weather AS (
    SELECT
        wr.*,
        f.match_datetime,
        dateDiff('second', wr.weather_datetime, f.match_datetime) AS diff_seconds,
        row_number() OVER (
            PARTITION BY wr.event_id
            ORDER BY abs(dateDiff('second', wr.weather_datetime, f.match_datetime)) ASC
        ) AS closest_rank,
        row_number() OVER (
            PARTITION BY wr.event_id
            ORDER BY 
                CASE 
                    WHEN wr.weather_datetime >= f.match_datetime THEN wr.weather_datetime
                    ELSE toDateTime('9999-12-31 00:00:00')
                END ASC
        ) AS after_rank
    FROM weather_raw AS wr
    INNER JOIN fixtures AS f
        ON wr.event_id = f.event_id
)

SELECT
    halfMD5(toString(event_id) || '|' || toString(toStartOfHour(weather_datetime))) AS weather_key,
    event_id,
    any(latitude) AS latitude,
    any(longitude) AS longitude,
    toStartOfHour(weather_datetime) AS timestamp_hour,
    max(match_datetime) AS match_datetime,
    round(avg(temperature), 3) AS temperature,
    round(avg(precipitation), 3) AS precipitation,
    round(avg(rainfall), 3) AS rainfall,
    round(avg(snowfall), 3) AS snowfall,
    round(avg(wind_speed), 3) AS wind_speed
FROM ranked_weather
WHERE closest_rank = 1 OR after_rank = 1
GROUP BY 
    event_id, 
    toStartOfHour(weather_datetime)