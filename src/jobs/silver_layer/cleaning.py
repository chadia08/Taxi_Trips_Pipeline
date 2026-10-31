
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

MAX_TRIP_SECONDS = 86_400  # 24 heures


def clean_trips(bronze_df: DataFrame) -> DataFrame:
    """
    Clean and normalize Chicago Taxi Trips data.

    Input: Bronze DataFrame
    Output: Silver DataFrame
    """

    df = bronze_df

    # 1. Vérification des colonnes nécessaires
    required_columns = [
        "trip_id",
        "trip_start_timestamp",
        "trip_end_timestamp",
        "trip_seconds",
        "trip_miles",
        "fare",
        "tips",
        "tolls",
        "extras",
        "trip_total",
    ]

    missing_columns = [
        column for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    # 2. Conversion explicite des types
    df = (
        df
        .withColumn(
            "trip_start_timestamp",
            F.to_timestamp("trip_start_timestamp")
        )
        .withColumn(
            "trip_end_timestamp",
            F.to_timestamp("trip_end_timestamp")
        )
        .withColumn("trip_seconds", F.col("trip_seconds").cast("long"))
        .withColumn("trip_miles", F.col("trip_miles").cast("double"))
    )

    numeric_columns = [
        "fare", "tips", "tolls", "extras", "trip_total"
    ]

    for column in numeric_columns:
        df = df.withColumn(
            column,
            F.regexp_replace(
                F.trim(F.col(column).cast("string")),
                r"[$,]",
                ""
            ).cast("double")
        )

    # 3. Conversion des colonnes géographiques si disponibles
    optional_numeric_columns = [
        "pickup_community_area",
        "dropoff_community_area",
        "pickup_centroid_latitude",
        "pickup_centroid_longitude",
        "dropoff_centroid_latitude",
        "dropoff_centroid_longitude",
    ]

    for column in optional_numeric_columns:
        if column in df.columns:
            target_type = (
                "int" if "community_area" in column else "double"
            )
            df = df.withColumn(
                column,
                F.col(column).cast(target_type)
            )

    # 4. Normalisation des chaînes de caractères
    if "payment_type" in df.columns:
        df = df.withColumn(
            "payment_type",
            F.initcap(F.trim(F.col("payment_type")))
        )

    if "company" in df.columns:
        df = df.withColumn(
            "company",
            F.trim(F.col("company"))
        )

    # 5. Suppression des doublons
    df = df.dropDuplicates(["trip_id"])

    # 6. Calcul du revenu
    zero = F.lit(0.0)

    any_component_present = (
        F.col("fare").isNotNull()
        | F.col("tips").isNotNull()
        | F.col("tolls").isNotNull()
        | F.col("extras").isNotNull()
    )

    fallback_sum = F.when(
        any_component_present,
        F.coalesce(F.col("fare"), zero)
        + F.coalesce(F.col("tips"), zero)
        + F.coalesce(F.col("tolls"), zero)
        + F.coalesce(F.col("extras"), zero)
    )

    df = df.withColumn(
        "revenue",
        F.coalesce(F.col("trip_total"), fallback_sum)
    )

    # 7. Colonnes dérivées
    df = (
        df
        .withColumn(
            "trip_duration_minutes",
            F.col("trip_seconds") / F.lit(60.0)
        )
        .withColumn(
            "pickup_date",
            F.to_date("trip_start_timestamp")
        )
        .withColumn(
            "pickup_hour",
            F.hour("trip_start_timestamp")
        )
    )

    # 8. Règles de qualité
    def non_negative_or_null(column):
        return (
            F.col(column).isNull()
            | (F.col(column) >= 0)
        )

    df = df.filter(
        F.col("trip_id").isNotNull()
        & F.col("trip_start_timestamp").isNotNull()
        & F.col("trip_seconds").isNotNull()
        & (F.col("trip_seconds") >= 0)
        & (F.col("trip_seconds") <= MAX_TRIP_SECONDS)
        & F.col("trip_miles").isNotNull()
        & (F.col("trip_miles") >= 0)
        & non_negative_or_null("fare")
        & non_negative_or_null("tips")
        & non_negative_or_null("tolls")
        & non_negative_or_null("extras")
        & non_negative_or_null("trip_total")
        & F.col("revenue").isNotNull()
        & (F.col("revenue") >= 0)
        & (
            F.col("trip_end_timestamp").isNull()
            | (
                F.col("trip_end_timestamp")
                >= F.col("trip_start_timestamp")
            )
        )
    )

    return df
