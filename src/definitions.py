from dagster import Definitions
from src.jobs.fetch_data import chicago_taxi_bronze
from src.jobs.silver_transform import chicago_taxi_silver

defs = Definitions(
    assets=[
        chicago_taxi_bronze,
        chicago_taxi_silver,
    ],
)