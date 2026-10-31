
from pyspark.sql import SparkSession
from dagster import asset
from src.jobs.silver_layer.cleaning import clean_trips


@asset(
    name="chicago_taxi_silver",
    group_name="silver",
    description="Nettoyage et structuration des données Chicago Taxi"
)
def chicago_taxi_silver(chicago_taxi_bronze):

    spark = (
        SparkSession.builder
        .appName("ChicagoTaxiSilverTransformation")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.5.0,"
            "software.amazon.awssdk:bundle:2.20.162"
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.connection.establish.timeout",
            "60000"
        )
        .config(
            "spark.hadoop.fs.s3a.connection.timeout",
            "60000"
        )
        .config(
            "spark.hadoop.fs.s3a.connection.request.timeout",
            "60000"
        )
        .getOrCreate()
    )

    bronze_path = (
        "s3a://datalake/bronze/taxi_trips/"
        "quarter=2023-Q1/raw_taxi_data.parquet"
    )

    silver_path = (
        "s3a://datalake/silver/taxi_trips/"
        "quarter=2023-Q1/"
    )

    try:
        print(f"Reading Bronze data: {bronze_path}")

        bronze_df = spark.read.parquet(bronze_path)

        print("Bronze schema:")
        bronze_df.printSchema()

        bronze_count = bronze_df.count()
        print(f"Bronze rows: {bronze_count}")

        # Application du nettoyage
        silver_df = clean_trips(bronze_df)

        silver_count = silver_df.count()
        print(f"Silver rows: {silver_count}")

        rejected_count = bronze_count - silver_count
        print(f"Rejected rows: {rejected_count}")

        if silver_count == 0:
            raise ValueError("Silver dataset is empty.")

        print(f"Writing Silver data to: {silver_path}")

        (
            silver_df.write
            .mode("overwrite")
            .parquet(silver_path)
        )

        print("Silver transformation completed successfully.")

        return silver_path

    finally:
        spark.stop()
