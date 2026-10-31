
SELECT
    pickup_date,
    total_trips,
    total_revenue,
    avg_revenue,
    avg_trip_distance,
    avg_trip_duration,
    quarter
FROM read_parquet(
    's3://datalake/gold/taxi_trips/quarter=2023-Q1/daily_trip_performance/*.parquet'
)
ORDER BY pickup_date ASC;
