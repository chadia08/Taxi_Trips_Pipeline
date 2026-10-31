from dagster import Definitions
from src.jobs.bronze_layer.fetch_data import chicago_taxi_bronze
from src.jobs.silver_layer.silver_transform import chicago_taxi_silver
from src.jobs.gold_layer.gold_transform import chicago_taxi_gold

defs = Definitions(
    assets=[
        chicago_taxi_bronze,
        chicago_taxi_silver,
        chicago_taxi_gold,
    ],
)
