from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col

SOURCE_PATH = (
    "/Volumes/business_lakehouse/landing/raw_volume/ecommerce/users/"
)

@dp.table(
    name="users_streaming",
    comment="Bronze streaming table for e-commerce users"
)
def users_streaming():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(SOURCE_PATH)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )