import pandas as pd
import numpy as np
import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta

cache_session = requests_cache.CachedSession('.cache_openmeteo', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

URL = "https://archive-api.open-meteo.com/v1/archive"

def fetch_weather_for_match(match_id, date, latitude, longitude):
    """Fetches historical weather data for a single match location and date."""
    
    start_date = date.strftime("%Y-%m-%d")
    end_date = start_date 
    
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ["temperature_2m", "precipitation", "rain", "snowfall", "wind_speed_10m"],
    }

    try:
        # API call
        responses = openmeteo.weather_api(URL, params=params)
        
        if not responses:
            print(f"Warning: No response for match {match_id} on {start_date}")
            return None

        response = responses[0]
        hourly = response.Hourly()
        
        hourly_data = {
            "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
            "precipitation": hourly.Variables(1).ValuesAsNumpy(),
            "rain": hourly.Variables(2).ValuesAsNumpy(),
            "snowfall": hourly.Variables(3).ValuesAsNumpy(),
            "wind_speed_10m": hourly.Variables(4).ValuesAsNumpy(),
        }

        time_index = pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        )
        
        hourly_df = pd.DataFrame(data = hourly_data, index = time_index)
        
        # Find the hour closest to the actual match time.
        target_time = pd.to_datetime(date).tz_localize('UTC', nonexistent='NaT')
        
        time_diff =np.abs(hourly_df.index - target_time)
        closest_time_label = time_diff.argmin()
        representative_hour = hourly_df.iloc[[closest_time_label]]
        
        result = representative_hour.iloc[0].to_dict()
        result['match_id'] = match_id
        
        # Ensure all values are Python types (float/int) and NaN is handled as None
        for key in result:
             if pd.isna(result[key]):
                result[key] = None
             elif isinstance(result[key], (float, int)):
                 pass
             else:
                 result[key] = result[key].item() if hasattr(result[key], 'item') else result[key]


        return result
        
    except Exception as e:
        print(f"Error fetching weather for match {match_id} ({start_date}): {e}")
        return None


def fetch_historical_weather(matches_with_coords_df):
    """
    Takes a DataFrame of matches with coordinates and fetches weather data for all.
    This respects the implicit rate limits by only running once per minute per DAG run.
    """
    
    results = []
    
    for index, row in matches_with_coords_df.iterrows():
        match_id = row['match_id']
        date = row['date_key']
        lat = row['latitude']
        lon = row['longitude']
        
        weather_data = fetch_weather_for_match(match_id, date, lat, lon)
        
        if weather_data:
            results.append(weather_data)
            
    if not results:
        return pd.DataFrame()
        
    return pd.DataFrame(results)