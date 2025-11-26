import os
import tempfile
import logging
import boto3
import pandas as pd
from clickhouse_driver import Client
import traceback
import pyiceberg
import s3fs
import pyarrow

log = logging.getLogger(__name__)

def _upload_parquet_to_minio(local_parquet_path: str, bucket: str, key: str):
    endpoint = os.environ.get("AWS_ENDPOINT")
    access = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name=os.environ.get("AWS_REGION"),
    )

    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        try:
            s3.create_bucket(Bucket=bucket)
        except Exception:
            log.warning("Could not create/check bucket; continue and attempt upload anyway.")

    with open(local_parquet_path, "rb") as f:
        s3.upload_fileobj(f, Bucket=bucket, Key=key)

    log.info("Uploaded parquet to s3://%s/%s", bucket, key)
    return f"s3://{bucket}/{key}"

def create_iceberg_table_from_csv(data_dir: str = "/opt/airflow/data/espn_soccer"):
    """
    1) Read fixtures.csv (small table example)
    2) Write parquet locally, upload to MinIO (practice-bucket/iceberg_exports/)
    3) (Optional) Register a minimal Iceberg table via PyIceberg REST catalog (best-effort)
    4) Create a ClickHouse read-only table/view using the Parquet stored in MinIO
    """
    fixtures_csv = os.path.join(data_dir, "fixtures.csv")
    if not os.path.exists(fixtures_csv):
        log.warning("fixtures.csv not found in %s — skipping Iceberg export", data_dir)
        return False

    df = pd.read_csv(fixtures_csv, dtype=str)
    if df.empty:
        log.warning("fixtures.csv is empty — skipping")
        return False

    fd, tmp_path = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    df.to_parquet(tmp_path, index=False)
    log.info("Wrote temp parquet: %s", tmp_path)

    bucket = os.environ.get("ICEBERG_BUCKET")
    print(f"Using Iceberg bucket: {bucket}")
    key = "iceberg_exports/bronze_fixtures.parquet"

    s3_uri = _upload_parquet_to_minio(tmp_path, bucket, key)

    try:
        from pyiceberg.catalog.rest import RestCatalog
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType
        from pyiceberg.exceptions import NoSuchTableError

        catalog_url = os.environ.get("PYICEBERG_CATALOG__REST__URI")

        catalog = RestCatalog(
            name="rest_catalog",
            **{
                "uri": catalog_url,
                "s3.endpoint": os.environ.get("AWS_ENDPOINT"),
                "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID"),
                "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
                "s3.region": os.environ.get("AWS_REGION"),
                "s3.path-style-access": "true",
            }
        )

        namespace = "default"
        table_name = "bronze_fixtures"
        schema_fields = []
        next_id = 1
        for col in df.columns:
            schema_fields.append(NestedField(next_id, col, StringType(), True))
            next_id += 1
        schema = Schema(*schema_fields)

        ident = ("default", table_name)
        try:
            catalog.load_table(ident)
            log.info("Iceberg table %s.%s already exists. Skipping creation.", ".".join(namespace), table_name)
        
        except NoSuchTableError:
            try:
                catalog.create_table(ident, schema=schema, location=f"s3://{bucket}/iceberg_exports/{table_name}")
                log.info("Created Iceberg table %s.%s via RestCatalog", ".".join(namespace), table_name)
            except Exception as e:
                log.warning("Could not create Iceberg table via PyIceberg: %s", e)
                log.error("Full traceback:\n%s", traceback.format_exc())
    except Exception as e:
        log.info("PyIceberg not available or registration failed (ok on dev): %s", e)
        log.error("Full traceback:\n%s", traceback.format_exc())

    # Create a ClickHouse read-only table/view which reads the Parquet from MinIO.
    try:
        client = Client(host="clickhouse-server")
        client.execute("CREATE DATABASE IF NOT EXISTS matchData")
        try:
            client.execute("DROP TABLE IF EXISTS matchData.iceberg_fixtures_readonly")
        except Exception:
            pass

        access = os.environ.get("AWS_ACCESS_KEY_ID")
        secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
        endpoint = os.environ.get("AWS_ENDPOINT")

        s3_url = f"http://minio:9000/{bucket}/{key}"
        create_view_sql = f"""
            CREATE VIEW IF NOT EXISTS matchData.iceberg_fixtures_readonly AS
            SELECT *
            FROM s3('{s3_url}','{access}','{secret}','Parquet');
        """
        client.execute(create_view_sql)
        log.info("Created ClickHouse view matchData.iceberg_fixtures_readonly -> %s", s3_url)
        client.disconnect()
    except Exception as e:
        log.warning("Could not create ClickHouse view for parquet: %s", e)

    # cleanup local parquet
    try:
        os.remove(tmp_path)
    except Exception:
        pass

    return True