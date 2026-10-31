import os
import time
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import boto3
from dagster import asset

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


@asset(
    name="chicago_taxi_bronze",
    group_name="bronze",
    description="Ingestion des trajets Chicago Taxi du premier trimestre 2023 vers MinIO"
)
def chicago_taxi_bronze():

    base_url = "https://data.cityofchicago.org/resource/wrvz-psew.json"

    start_date = "2023-01-01T00:00:00"
    end_date = "2023-04-01T00:00:00"

    limit = 10000
    offset = 0

    all_batches = []
    total_rows = 0
    max_retries = 5

    print("Starting Chicago Taxi Q1 2023 ingestion...")

    while True:

        params = {
            "$where": (
                f"trip_start_timestamp >= '{start_date}' "
                f"AND trip_start_timestamp < '{end_date}'"
            ),
            "$limit": limit,
            "$offset": offset,
            "$order": "trip_start_timestamp ASC"
        }

        # API request with retry mechanism
        for attempt in range(max_retries):

            try:
                response = requests.get(
                    base_url,
                    params=params,
                    timeout=(15, 300)
                )

                response.raise_for_status()
                batch = response.json()
                break

            except requests.exceptions.RequestException as error:

                if attempt == max_retries - 1:
                    raise Exception(
                        f"API request failed after {max_retries} attempts. "
                        f"Offset: {offset}. Error: {error}"
                    ) from error

                wait_time = 2 ** attempt

                print(
                    f"Request failed at offset {offset}. "
                    f"Attempt {attempt + 1}/{max_retries}. "
                    f"Retrying in {wait_time} seconds..."
                )

                time.sleep(wait_time)

            except ValueError as error:

                raise Exception(
                    f"Invalid JSON response at offset {offset}"
                ) from error

        if not batch:
            print("No more data returned by API.")
            break

        df_batch = pd.DataFrame(batch)
        all_batches.append(df_batch)

        batch_rows = len(df_batch)
        total_rows += batch_rows
        offset += batch_rows

        print(
            f"Fetched {batch_rows} rows | "
            f"Total: {total_rows} | Offset: {offset}"
        )

        if batch_rows < limit:
            break

    if total_rows == 0:
        raise Exception("No data returned by Chicago Taxi API.")

    df = pd.concat(all_batches, ignore_index=True)

    print(f"Total rows fetched: {len(df)}")

    # Conversion en Parquet
    table = pa.Table.from_pandas(df, preserve_index=False)

    buffer = BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    # Upload vers MinIO
    s3 = get_s3_client()

    file_path = (
        "bronze/taxi_trips/quarter=2023-Q1/raw_taxi_data.parquet"
    )

    print(f"Uploading to s3://{BUCKET_NAME}/{file_path}")

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_path,
        Body=buffer.getvalue()
    )

    print("Bronze ingestion completed successfully.")

    return file_path