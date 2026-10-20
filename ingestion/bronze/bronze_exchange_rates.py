from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col

SOURCE_PATH = (
    "/Volumes/business_lakehouse/landing/raw_volume/exchange_rates/"
)


@dp.table(
    name="exchange_rates_streaming",
    comment="Bronze streaming table for exchange rates"
)
def exchange_rates_streaming():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "xml")
        .option("rowTag", "Cube")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(SOURCE_PATH)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )