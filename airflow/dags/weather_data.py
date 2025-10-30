from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from clickhouse_driver import Client
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
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
    log.info("Starting geocoding process for unresolved venues...")

    client = None
    updates_count = 0

    try:
        client = Client(host="clickhouse-server")
        client.execute("USE matchData")

        geolocator = Nominatim(user_agent="espn_soccer_airflow_dag_v2", timeout=10)
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1) 
        
        VENUE_BATCH_SIZE = 50
        unresolved = client.execute(f"""
            SELECT v.venueId, v.fullName, v.city, v.country
            FROM bronze_venues v
            JOIN bronze_fixtures f ON v.venueId = f.venueId
            WHERE v.latitude IS NULL
            GROUP BY v.venueId, v.fullName, v.city, v.country
            ORDER BY count() DESC
            LIMIT {VENUE_BATCH_SIZE}
        """)

        if not unresolved:
            log.info("No venues found requiring geocoding. Exiting.")
            return

        log.info(f"Found {len(unresolved)} venues to geocode in this run.")

        for venue_id, venue_name, city, country in unresolved:
            q_parts = [
                city.strip() if city and city.lower() != "none" else None,
                country.strip() if country and country.lower() != "none" else None,
            ]
            q = ", ".join(filter(None, q_parts))
            try:
                loc = geocode(q, language="en")
                if loc:
                    lat = float(loc.latitude)
                    lon = float(loc.longitude)
                    
                    client.execute(f"""
                        ALTER TABLE bronze_venues
                        UPDATE latitude = {lat}, longitude = {lon}
                        WHERE venueId = '{str(venue_id)}'
                        SETTINGS mutations_sync = 1
                    """)
                    updates_count += 1
                    log.info(f"Geocoded and updated {venue_id}: {q} -> ({lat}, {lon})")
                else:
                    log.warning(f"Could not geocode venue ID {venue_id} for '{q}'")
            except Exception as e:
                log.error(f"Error executing update for venue ID {venue_id}: {e}")
                time.sleep(5)
                continue
        
        log.info(f"Successfully updated {updates_count} venues.")

    except Exception as e:
        log.error(f"A critical error occurred during geocoding: {e}")
        raise
    
    finally:
        if client:
            client.disconnect()
            log.info("ClickHouse client disconnected.")

    log.info("Geocoding task complete.")


def fetch_and_load_weather_data(data_dir, **context):
    try:
        import open_meteo_client as omc
    except ImportError:
        raise ImportError("ERROR: open_meteo_client.py is required but not found.")

    BATCH_SIZE = 600 # opean_meteo minute limit
    client = None
    try:
        client = Client(host="clickhouse-server")
        client.execute("USE matchData")

        # Ensure target table exists
        client.execute("""
            CREATE TABLE IF NOT EXISTS bronze_weather
            (
              eventId String,
              date Date,
              timestamp_utc DateTime('UTC'),
              latitude Float64,
              longitude Float64,
              temperature_2m Nullable(Float32),
              precipitation Nullable(Float32),
              rain Nullable(Float32),
              snowfall Nullable(Float32),
              wind_speed_10m Nullable(Float32)
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (eventId, timestamp_utc)
        """)

        rows = client.execute(f"""
            SELECT f.eventId, f.date, v.latitude, v.longitude
            FROM bronze_fixtures f
            JOIN bronze_venues v ON f.venueId = v.venueId
            WHERE f.hasWeather IS NULL
              AND v.latitude IS NOT NULL
            ORDER BY f.date ASC
            LIMIT {BATCH_SIZE}
        """)

        if not rows:
            log.info("No matches found requiring weather data with coordinates. Exiting.")
            return

        matches_df = pd.DataFrame(rows, columns=['eventId', 'date', 'latitude', 'longitude'])
        log.info(f"Fetching weather for {len(matches_df)} matches from Open-Meteo...")

        weather_df = omc.fetch_weather_bronze(matches_df)
        if weather_df.empty:
            log.info("Weather API returned no rows. Exiting.")
            return

        to_insert = [
            (
                str(r["eventId"]),
                pd.Timestamp(r["date"]).date(),
                pd.Timestamp(r["timestamp_utc"]).to_pydatetime(),
                float(r["latitude"]),
                float(r["longitude"]),
                float(r["temperature_2m"]) if pd.notnull(r["temperature_2m"]) else None,
                float(r["precipitation"]) if pd.notnull(r["precipitation"]) else None,
                float(r["rain"]) if pd.notnull(r["rain"]) else None,
                float(r["snowfall"]) if pd.notnull(r["snowfall"]) else None,
                float(r["wind_speed_10m"]) if pd.notnull(r["wind_speed_10m"]) else None,
            )
            for _, r in weather_df.iterrows()
        ]

        client.execute(
            """
            INSERT INTO bronze_weather (
                eventId, date, timestamp_utc, latitude, longitude,
                temperature_2m, precipitation, rain, snowfall, wind_speed_10m
            ) VALUES
            """,
            to_insert,
        )
        log.info(f"Inserted {len(to_insert)} hourly rows into bronze_weather")

        # Mark fixtures as having weather
        event_ids_done = matches_df["eventId"].unique().tolist()
        if event_ids_done:
            CHUNK = 1000
            for i in range(0, len(event_ids_done), CHUNK):
                ids_csv = ",".join([f"'{str(x)}'" for x in event_ids_done[i:i+CHUNK]])
                client.execute(f"""
                    ALTER TABLE bronze_fixtures
                    UPDATE hasWeather = 1
                    WHERE eventId IN ({ids_csv})
                    SETTINGS mutations_sync = 1
                """)
            log.info(f"Updated hasWeather=1 for {len(event_ids_done)} fixtures")

    except Exception as e:
        log.error(f"A critical error occurred during weather data processing: {e}")
        raise 
        
    finally:
        if client:
            client.disconnect()
            log.info("ClickHouse client disconnected.")

    log.info("Weather ingestion complete.")


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