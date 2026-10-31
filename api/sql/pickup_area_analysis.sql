
SELECT
    pickup_community_area,
    total_trips,
    total_revenue,
    avg_trip_distance,
    avg_trip_duration,
    quarter
FROM read_parquet(
    's3://datalake/gold/taxi_trips/quarter=2023-Q1/pickup_area_analysis/*.parquet'
)
ORDER BY total_trips DESC;
