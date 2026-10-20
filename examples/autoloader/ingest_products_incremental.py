from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

# Configuration
SOURCE_PATH = (
    "/Volumes/business_lakehouse/landing/raw_volume/"
    "ecommerce/products"
)

SCHEMA_PATH = (
    "/Volumes/business_lakehouse/landing/raw_volume/"
    "_schemas/products_incremental"
)

CHECKPOINT_PATH = (
    "/Volumes/business_lakehouse/landing/raw_volume/"
    "_checkpoints/products_incremental"
)

TARGET_TABLE = "business_lakehouse.bronze.products_incremental"

def main():
    spark = SparkSession.builder.getOrCreate()

    # Read new files using Auto Loader
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(SOURCE_PATH)
    )

    # Add ingestion metadata
    df_bronze = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

    # Incremental write to Delta table
    query = (
        df_bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(TARGET_TABLE)
        )
    query.awaitTermination()

    print(f"Incremental ingestion completed: {TARGET_TABLE}")

if __name__ == "__main__":
    main()    




 




