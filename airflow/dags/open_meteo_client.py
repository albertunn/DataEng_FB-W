import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry

cache_session = requests_cache.CachedSession('.cache_openmeteo', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

URL = "https://archive-api.open-meteo.com/v1/archive"

def fetch_weather_bronze(matches_df: pd.DataFrame) -> pd.DataFrame:
    frames = []

    for _, row in matches_df.iterrows():
        match_id = row['eventId']
        date = pd.to_datetime(row['date']).date()
        lat = float(row['latitude'])
        lon = float(row['longitude'])

        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": f"{date:%Y-%m-%d}",
            "end_date": f"{date:%Y-%m-%d}",
            "hourly": ["temperature_2m", "precipitation", "rain", "snowfall", "wind_speed_10m"],
            "timezone": "UTC"
        }

        try:
            responses = openmeteo.weather_api(URL, params=params)
            if not responses:
                continue

            hourly = responses[0].Hourly()

            idx = pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )

            df_h = pd.DataFrame({
                "eventId": match_id,
                "date": pd.Timestamp(date),
                "latitude": lat,
                "longitude": lon,
                "timestamp_utc": idx,
                "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
                "precipitation":  hourly.Variables(1).ValuesAsNumpy(),
                "rain":           hourly.Variables(2).ValuesAsNumpy(),
                "snowfall":       hourly.Variables(3).ValuesAsNumpy(),
                "wind_speed_10m": hourly.Variables(4).ValuesAsNumpy()
            })

            frames.append(df_h)

        except Exception as e:
            print(f"Error fetching weather for match {match_id}: {e}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()