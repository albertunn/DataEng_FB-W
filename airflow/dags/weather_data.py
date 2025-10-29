from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor 
import pandas as pd
from clickhouse_driver import Client
from geopy.geocoders import Nominatim
import time # Needed for rate limiting Nominatim
import logging

# Set up logging
log = logging.getLogger(__name__)

# Default arguments for the DAG
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
    
    # Initialize ClickHouse Client
    client = Client(host="clickhouse-server")
    client.execute("USE matchData")
    
    # Initialize Geocoder with a distinct user agent
    geolocator = Nominatim(user_agent="espn_soccer_airflow_dag_v2")

    # Define the batch size for geocoding to manage the 1.1s latency
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

    log.info(f"Found {len(unresolved_venues)} high-priority venues to geocode in this run.")
    
    # 2. Geocode and update
    for venue_id, venue_name, city, country in unresolved_venues:
        location_query = f"{venue_name}, {city}, {country}"
        
        try:
            # CRITICAL: Respecting the Nominatim rate limit (1 request per second minimum)
            time.sleep(1.1) 
            
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
                
        except Exception as e:
            log.error(f"FATAL Error geocoding venue ID {venue_id}: {e}")
            time.sleep(10) 
            break 
            
    client.disconnect()
    log.info("Geocoding task complete.")
    

def fetch_and_load_weather_data(data_dir, **context):
    """
    1. Connects to ClickHouse.
    2. Identifies up to 600 fixtures without weather data.
    3. Calls the rate-limited Open-Meteo API to get weather data.
    4. Inserts results into WeatherFact and updates MatchFact.
    """
    
    # Placeholder for the imported client
    try:
        import open_meteo_client as omc
    except ImportError:
        log.error("ERROR: open_meteo_client.py not found. Skipping execution.")
        return 

    BATCH_SIZE = 600 
    
    log.info("Connecting to ClickHouse and identifying fixtures for weather lookup...")
    client = Client(host="clickhouse-server")
    client.execute("USE matchData")

    # 1. Select matches needing weather data, joining with DimVenue to get coordinates
    # We only select matches where coordinates are already available (latitude IS NOT NULL)
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

    # Convert to DataFrame 
    matches_with_coords = pd.DataFrame(matches_to_fetch, columns=['match_id', 'date_key', 'latitude', 'longitude'])
    
    # 2. Fetch weather data using the client utility
    log.info(f"Fetching weather for {len(matches_with_coords)} matches from Open-Meteo...")
    
    weather_results_df = omc.fetch_historical_weather(matches_with_coords)
    
    # 3. Update ClickHouse (Create table if needed, insert data, and update MatchFact)
    if not weather_results_df.empty:
        
        # A. Create WeatherFact table (Safe to run every time)
        client.execute("""
        CREATE TABLE IF NOT EXISTS DimWeather (
            match_id UInt32,
            temperature_2m Nullable(Float32),
            precipitation Nullable(Float32),
            rain Nullable(Float32),
            snowfall Nullable(Float32),
            wind_speed_10m Nullable(Float32)
        ) ENGINE = MergeTree() ORDER BY match_id
        """)
        
        # B. Prepare data for dimWeather
        dim_weather_data = [
            (int(row['match_id']), row['temperature_2m'], row['precipitation'], row['rain'], row['snowfall'], row['wind_speed_10m'])
            for index, row in weather_results_df.iterrows()
        ]
        
        # C. Insert into dimWeather
        client.execute("INSERT INTO DimWeather VALUES", dim_weather_data)
        
        # D. Update MatchFact (set weather_id = 1)
        match_ids_processed = [row['match_id'] for index, row in weather_results_df.iterrows()]
        match_ids_str = ','.join(map(str, match_ids_processed))

        if match_ids_str:
             client.execute(f"ALTER TABLE MatchFact UPDATE weather_id = 1 WHERE match_id IN ({match_ids_str}) SETTINGS mutations_sync = 1")

        log.info(f"Inserted {len(dim_weather_data)} records into DimWeather and updated MatchFact.")
        
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
