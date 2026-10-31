
from dagster import asset
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@asset(
    name="chicago_taxi_gold",
    group_name="gold",
    description="Création des tables analytiques Gold à partir des données Silver."
)
def chicago_taxi_gold(chicago_taxi_silver):

    spark = (
        SparkSession.builder
        .appName("ChicagoTaxiGoldTransformation")
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
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
        .getOrCreate()
    )

    silver_path = (
        "s3a://datalake/silver/taxi_trips/"
        "quarter=2023-Q1/"
    )

    gold_base_path = (
        "s3a://datalake/gold/taxi_trips/quarter=2023-Q1/"
    )

    try:
        print(f"Reading Silver data: {silver_path}")

        df = spark.read.parquet(silver_path)

        # Vérification des colonnes nécessaires
        required_columns = {
            "pickup_date",
            "pickup_hour",
            "revenue",
            "trip_miles",
            "trip_duration_minutes",
            "payment_type",
            "tips",
            "pickup_community_area",
        }

        missing_columns = required_columns - set(df.columns)

        if missing_columns:
            raise ValueError(
                f"Missing required columns: {sorted(missing_columns)}"
            )

        silver_count = df.count()
        print(f"Silver rows: {silver_count}")

        if silver_count == 0:
            raise ValueError("Silver dataset is empty.")

        # 1. Performance quotidienne
        daily_trip_performance = (
            df.groupBy("pickup_date")
            .agg(
                F.count("*").alias("total_trips"),
                F.sum("revenue").alias("total_revenue"),
                F.avg("revenue").alias("avg_revenue"),
                F.avg("trip_miles").alias("avg_trip_distance"),
                F.avg("trip_duration_minutes").alias("avg_trip_duration"),
            )
        )

        # 2. Analyse par moyen de paiement
        payment_analysis = (
            df.groupBy("payment_type")
            .agg(
                F.count("*").alias("total_trips"),
                F.sum("revenue").alias("total_revenue"),
                F.avg("revenue").alias("avg_revenue"),
                F.sum("tips").alias("total_tips"),
            )
        )

        # 3. Analyse par zone de prise en charge
        pickup_area_analysis = (
            df.filter(F.col("pickup_community_area").isNotNull())
            .groupBy("pickup_community_area")
            .agg(
                F.count("*").alias("total_trips"),
                F.sum("revenue").alias("total_revenue"),
                F.avg("trip_miles").alias("avg_trip_distance"),
                F.avg("trip_duration_minutes").alias("avg_trip_duration"),
            )
        )

        # 4. Analyse par heure
        hourly_trip_analysis = (
            df.groupBy("pickup_hour")
            .agg(
                F.count("*").alias("total_trips"),
                F.sum("revenue").alias("total_revenue"),
                F.avg("trip_duration_minutes").alias("avg_trip_duration"),
            )
        )

        # Écriture des quatre datasets
        datasets = {
            "daily_trip_performance": daily_trip_performance,
            "payment_analysis": payment_analysis,
            "pickup_area_analysis": pickup_area_analysis,
            "hourly_trip_analysis": hourly_trip_analysis,
        }

        results = {}

        for name, dataset in datasets.items():
            output_path = f"{gold_base_path}{name}/"

            row_count = dataset.count()

            if row_count == 0:
                raise ValueError(
                    f"Gold dataset {name} is empty."
                )

            print(f"Writing {name}: {row_count} rows")
            print(f"Output path: {output_path}")

            (
                dataset.write
                .mode("overwrite")
                .parquet(output_path)
            )

            results[name] = {
                "path": output_path,
                "rows": row_count,
            }

        print("All Gold datasets created successfully.")

        return results

    finally:
        spark.stop()
