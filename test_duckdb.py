
import duckdb

con = duckdb.connect()

con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

con.execute("""
    CREATE SECRET minio_secret (
        TYPE S3,
        KEY_ID 'minioadmin',
        SECRET 'minioadmin',
        REGION 'us-east-1',
        ENDPOINT 'localhost:9002',
        URL_STYLE 'path',
        USE_SSL false
    )
""")

with open("dashboard/sql/hourly_trip_analysis.sql", encoding="utf-8") as file:
    query = file.read()

result = con.execute(query).df()

print(result.head())
print("\nNombre de lignes :", len(result))
