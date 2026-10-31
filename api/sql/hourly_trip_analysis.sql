
SELECT
    pickup_hour,
    total_trips,
    total_revenue,
    avg_trip_duration,
    quarter
FROM read_parquet(
    's3://datalake/gold/taxi_trips/quarter=2023-Q1/hourly_trip_analysis/*.parquet'
)
ORDER BY pickup_hour ASC;
