from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

# Configuration

SOURCE_PATH = (
    "/Volumes/business_lakehouse/landing/raw_volume/ecommerce/users"
)

TARGET_TABLE = "business_lakehouse.bronze.users"

def main():
    spark = SparkSession.builder.getOrCreate()

    # Read source JSON
    df = spark.read.json(SOURCE_PATH)

    # Add ingestion metadata
    df_bronze = (
        df
        .withColumn('_ingested_at', current_timestamp())
        .withColumn('_source_file', col('_metadata.file_path'))
    )

    # Write to Delta table
    (
        df_bronze.write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(TARGET_TABLE)
    )

    print(f"Successfully loaded data into table: {TARGET_TABLE}")

if __name__ == "__main__":
    main()