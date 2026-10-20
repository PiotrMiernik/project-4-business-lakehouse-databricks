from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

SOURCE_PATH = (
    "/Volumes/business_lakehouse/landing/raw_volume/"
    "ecommerce/products"
)

SCHEMA_PATH = (
    "/Volumes/business_lakehouse/landing/raw_volume/"
    "_schemas/products_rescue_test"
)

CHECKPOINT_PATH = (
    "/Volumes/business_lakehouse/landing/raw_volume/"
    "_checkpoints/products_rescue_test"
)

TARGET_TABLE = "business_lakehouse.bronze.products_rescue_test"


def main():
    spark = SparkSession.builder.getOrCreate()

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .load(SOURCE_PATH)
    )

    df_bronze = (
        df.withColumn("_ingested_at", current_timestamp())
          .withColumn("_source_file", col("_metadata.file_path"))
    )

    query = (
        df_bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(TARGET_TABLE)
    )

    query.awaitTermination()

    print(f"Rescue ingestion completed: {TARGET_TABLE}")


if __name__ == "__main__":
    main()