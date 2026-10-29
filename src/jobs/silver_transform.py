from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dagster import asset


@asset(
    name="chicago_taxi_silver",
    group_name="silver",
    description="Nettoyage et structuration des données Chicago Taxi (Couche Silver avec PySpark natif MinIO)"
)
def chicago_taxi_silver(chicago_taxi_bronze):
    # Initialisation de Spark avec Hadoop-AWS 3.5.0 et son SDK AWS v2 obligatoire
    spark = (
        SparkSession.builder
        .appName("ChicagoTaxiSilverTransformation")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.5.0,software.amazon.awssdk:bundle:2.20.162"
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
        .config("spark.hadoop.fs.s3a.committer.name", "directory")
        # Surcharges numériques strictes pour contrer les timeouts textuels par défaut de Hadoop 3.5.0
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
        print(f"Lecture des données Bronze depuis MinIO : {bronze_path}")
        df = spark.read.parquet(bronze_path)

        # 1. Suppression des lignes avec des timestamps critiques nuls
        cleaned_df = df.dropna(
            subset=["trip_start_timestamp", "trip_end_timestamp"]
        )

        # 2. Filtrage des montants de course négatifs
        if "fare" in cleaned_df.columns:
            cleaned_df = cleaned_df.filter(col("fare") >= 0)

        # 3. Écriture des données nettoyées dans la couche Silver
        print(f"Écriture des données nettoyées vers MinIO : {silver_path}")

        cleaned_df.write.mode("overwrite").parquet(silver_path)

        print("Couche Silver générée et validée avec succès !")

        return silver_path

    finally:
        spark.stop()