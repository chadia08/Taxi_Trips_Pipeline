import os
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import boto3
from dagster import asset, Definitions

# Configuration S3 / MinIO
S3_ENDPOINT = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9002")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
BUCKET_NAME = "datalake"

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )

@asset(name="chicago_taxi_bronze", group_name="bronze", description="Ingestion d'un petit échantillon de test Chicago Taxi vers MinIO")
def chicago_taxi_bronze():
    base_url = "https://data.cityofchicago.org/resource/wrvz-psew.json"
    start_date = "2023-01-01T00:00:00"
    end_date = "2023-01-02T00:00:00" # Limité à une seule journée pour le test rapide
    
    limit = 1000  # Petit volume pour accélérer les tests
    
    print(f"Fetching a small sample ({limit} rows) of Chicago Taxi trips...")
    
    query_url = (
        f"{base_url}?"
        f"$where=trip_start_timestamp >= '{start_date}' AND trip_start_timestamp < '{end_date}'"
        f"&$limit={limit}"
        f"&$order=trip_start_timestamp ASC"
    )
    
    response = requests.get(query_url)
    
    if response.status_code != 200:
        raise Exception(f"API Error {response.status_code}: {response.text}")
    
    batch = response.json()
    df = pd.DataFrame(batch)
    print(f"Successfully fetched a sample of {len(df)} rows.")
    
    # Upload vers MinIO (Bronze)
    s3 = get_s3_client()
    table = pa.Table.from_pandas(df)
    buffer = BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    
    file_path = "bronze/taxi_trips/quarter=2023-Q1/raw_taxi_data.parquet"
    print(f"Uploading raw slice to s3://{BUCKET_NAME}/{file_path}...")
    
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_path,
        Body=buffer.getvalue()
    )
    print("Bronze ingestion complete for the sample slice.")
    return file_path

# Déclaration officielle pour Dagster
defs = Definitions(
    assets=[chicago_taxi_bronze]
)