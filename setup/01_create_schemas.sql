-- Project 4: Business Lakehouse on Databricks
-- Initialize Unity Catalog schemas for Medallion Architecture

-- Bronze Layer
CREATE SCHEMA IF NOT EXISTS business_lakehouse.bronze;

-- Silver Layer
CREATE SCHEMA IF NOT EXISTS business_lakehouse.silver;

-- Gold Layer
CREATE SCHEMA IF NOT EXISTS business_lakehouse.gold;

-- Verify schemas
SHOW SCHEMAS IN business_lakehouse; 