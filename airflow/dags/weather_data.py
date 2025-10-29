from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor 
import pandas as pd
from clickhouse_driver import Client
from geopy.geocoders import Nominatim
import time
import logging

log = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def geocode_unresolved_venues():
    """
    Uses geopy (Nominatim) to find coordinates for venues where latitude is NULL.
    CRITICAL CHANGE: This now prioritizes the top 50 most-used venues 
    to quickly unblock the majority of the match data.
    """
    log.info("Starting geocoding process for unresolved venues...")
    
    client = Client(host="clickhouse-server")
    client.execute("USE matchData")
    
    geolocator = Nominatim(user_agent="espn_soccer_airflow_dag_v2")

    # Batch size for geocoding to manage the 1s latency so that the task doesnt take too much time
    VENUE_BATCH_SIZE = 50 

    # 1. Select the top N most-used venues that are missing coordinates (latitude IS NULL)
    query = f"""
    SELECT
        v.venue_id, v.venue_name, v.city, v.country
    FROM DimVenue v
    JOIN MatchFact m ON v.venue_id = m.venue_id
    WHERE v.latitude IS NULL
    GROUP BY v.venue_id, v.venue_name, v.city, v.country
    ORDER BY count() DESC  -- Prioritize by most frequently used venues
    LIMIT {VENUE_BATCH_SIZE}
    """
    
    unresolved_venues = client.execute(query)
    
    if not unresolved_venues:
        log.info("No venues found requiring geocoding. Exiting.")
        return

    log.info(f"Found {len(unresolved_venues)} venues to geocode in this run.")
    
    for venue_id, venue_name, city, country in unresolved_venues:
        location_query = f"{venue_name}, {city}, {country}"
        
        try:
            # Respecting the Nominatim rate limit (1 request per second max)
            time.sleep(1) 
            
            location = geolocator.geocode(location_query, timeout=10)
            
            if location:
                lat = location.latitude
                lon = location.longitude
                log.info(f"Geocoded Venue ID {venue_id}: {city} -> ({lat}, {lon})")
                
                # Update DimVenue in ClickHouse
                client.execute(f"""
                ALTER TABLE DimVenue UPDATE latitude = {lat}, longitude = {lon}
                WHERE venue_id = {venue_id} SETTINGS mutations_sync = 1
                """)
            else:
                log.warning(f"Could not geocode venue ID {venue_id} for '{location_query}'")
                try:
                    print(location, location.latitude, location.longitude)
                except:
                    pass
        except Exception as e:
            log.error(f"FATAL Error geocoding venue ID {venue_id}: {e}")
            time.sleep(10) 
            break 
            
    client.disconnect()
    log.info("Geocoding task complete.")
    

def fetch_and_load_weather_data(data_dir, **context):
    """
    2. Identifies up to 600 fixtures without weather data. (API gives 600 queries per minute)
    3. Calls the Open-Meteo API to get weather data.
    4. Inserts results into WeatherFact and updates MatchFact.
    """
    
    try:
        import open_meteo_client as omc
    except ImportError:
        log.error("ERROR: open_meteo_client.py not found. Skipping execution.")
        return 

    BATCH_SIZE = 600 
    
    log.info("Connecting to ClickHouse and identifying fixtures for weather lookup...")
    client = Client(host="clickhouse-server")
    client.execute("USE matchData")

    # Select matches needing weather data, joining with DimVenue to get coordinates
    # Only select matches where coordinates are already available
    query = f"""
    SELECT
        m.match_id, m.date_key, v.latitude, v.longitude
    FROM MatchFact m
    JOIN DimVenue v ON m.venue_id = v.venue_id
    WHERE m.weather_id = 0 
      AND m.venue_id IS NOT NULL 
      AND v.latitude IS NOT NULL
    ORDER BY m.date_key ASC
    LIMIT {BATCH_SIZE}
    """
    
    matches_to_fetch = client.execute(query)
    
    if not matches_to_fetch:
        log.info("No matches found requiring weather data with coordinates. Exiting.")
        return
        
    log.info(f"Found {len(matches_to_fetch)} matches to process in this batch.")

    matches_with_coords = pd.DataFrame(matches_to_fetch, columns=['match_id', 'date_key', 'latitude', 'longitude'])
    
    log.info(f"Fetching weather for {len(matches_with_coords)} matches from Open-Meteo")
    
    weather_results_df = omc.fetch_historical_weather(matches_with_coords)
    
    if not weather_results_df.empty:
        
        client.execute("""
        CREATE TABLE IF NOT EXISTS DimWeather (
            weather_id UInt32,
            temperature_2m Nullable(Float32),
            precipitation Nullable(Float32),
            rain Nullable(Float32),
            snowfall Nullable(Float32),
            wind_speed_10m Nullable(Float32)
        ) ENGINE = MergeTree() ORDER BY weather_id
        """)
        
    result = client.execute("SELECT max(weather_id) FROM DimWeather")
    last_id = result[0][0] if result and result[0][0] is not None else 0
    next_id = last_id + 1

    dim_weather_data = []
    match_weather_map = {}

    for _, row in weather_results_df.iterrows():
        weather_id = next_id
        next_id += 1

        dim_weather_data.append((
            int(weather_id),
            row.get('temperature_2m'),
            row.get('precipitation'),
            row.get('rain'),
            row.get('snowfall'),
            row.get('wind_speed_10m') or row.get('windspeed_10m'),
        ))

        match_weather_map[int(row['match_id'])] = weather_id

    client.execute("INSERT INTO DimWeather VALUES", dim_weather_data)

    for match_id, weather_id in match_weather_map.items():
        client.execute(f"""
        ALTER TABLE MatchFact UPDATE weather_id = {weather_id}
        WHERE match_id = {match_id} SETTINGS mutations_sync = 1
        """)

    log.info(f"Inserted {len(dim_weather_data)} records into DimWeather")
    client.disconnect()



with DAG(
    dag_id='fetch_match_weather',
    default_args=default_args,
    description='Fetches historical weather for matches using geocoding and rate-limited Open-Meteo API.',
    schedule=None,
    catchup=False,
    tags=['weather', 'soccer', 'api'],
) as dag:

    geocode_venues = PythonOperator(
        task_id='geocode_venues',
        python_callable=geocode_unresolved_venues,
    )
    
    fetch_weather_task = PythonOperator(
        task_id='fetch_and_load_weather_data',
        python_callable=fetch_and_load_weather_data,
        op_kwargs={'data_dir': '/opt/airflow/data/espn_soccer'},
    )

    geocode_venues >> fetch_weather_task
