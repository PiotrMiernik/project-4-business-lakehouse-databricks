from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col

SOURCE_PATH = (
    "/Volumes/business_lakehouse/landing/raw_volume/"
    "ecommerce/products/"
)

@dp.table(
    name="products_streaming",
    comment="Bronze streaming table for e-commerce products"
)
def products_streaming():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(SOURCE_PATH)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )