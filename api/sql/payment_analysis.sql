
SELECT
    payment_type,
    total_trips,
    total_revenue,
    avg_revenue,
    total_tips,
    quarter
FROM read_parquet(
    's3://datalake/gold/taxi_trips/quarter=2023-Q1/payment_analysis/*.parquet'
)
ORDER BY total_trips DESC;
