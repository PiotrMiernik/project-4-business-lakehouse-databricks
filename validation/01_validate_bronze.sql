-- Validate Bronze ingestion results

SELECT
    'products' AS table_name,
    COUNT(*) AS row_count,
    SUM(size(products)) AS source_records,
    COUNT_IF(_source_file IS NULL) AS missing_source_file,
    COUNT_IF(_ingested_at IS NULL) AS missing_ingestion_time
FROM business_lakehouse.bronze.products

UNION ALL

SELECT
    'users',
    COUNT(*),
    SUM(size(users)),
    COUNT_IF(_source_file IS NULL),
    COUNT_IF(_ingested_at IS NULL)
FROM business_lakehouse.bronze.users

UNION ALL

SELECT
    'orders',
    COUNT(*),
    SUM(size(orders)),
    COUNT_IF(_source_file IS NULL),
    COUNT_IF(_ingested_at IS NULL)
FROM business_lakehouse.bronze.orders

UNION ALL

SELECT
    'exchange_rates',
    COUNT(*),
    SUM(size(Cube)),
    COUNT_IF(_source_file IS NULL),
    COUNT_IF(_ingested_at IS NULL)
FROM business_lakehouse.bronze.exchange_rates;

DESCRIBE DETAIL business_lakehouse.bronze.products;
DESCRIBE DETAIL business_lakehouse.bronze.users;
DESCRIBE DETAIL business_lakehouse.bronze.orders;

DESCRIBE HISTORY business_lakehouse.bronze.orders;
DESCRIBE HISTORY business_lakehouse.bronze.users;
DESCRIBE HISTORY business_lakehouse.bronze.products;