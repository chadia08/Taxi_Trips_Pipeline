
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("InspectSilver")
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

try:
    silver_path = (
        "s3a://datalake/silver/taxi_trips/"
        "quarter=2023-Q1/"
    )

    df = spark.read.parquet(silver_path)

    print("\n========== COLONNES SILVER ==========")
    df.printSchema()

    print("\n========== NOMBRE DE LIGNES ==========")
    print(df.count())

    print("\n========== APERÇU DES DONNÉES ==========")
    df.show(5, truncate=False)
    

    print("\n========== PAYMENT TYPES ==========")
    df.groupBy("payment_type").count().orderBy(
        F.desc("count")
    ).show(50, truncate=False)

    print("\n========== NULL VALUES ==========")
    df.select([
        F.count(
            F.when(F.col(c).isNull(), c)
        ).alias(c)
        for c in df.columns
    ]).show(truncate=False)

    print("\n========== DUPLICATE TRIP IDS ==========")
    print(
        df.count() - df.select("trip_id").distinct().count()
    )

finally:
    spark.stop()
