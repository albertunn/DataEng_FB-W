SELECT 
weather_key, 
latitude,
longitude,
weather_date,
weather_time,
temperature,
precipitation,
rainfall,
snowfall,
wind_speed

FROM {{ ref('stg_dim_weather') }}