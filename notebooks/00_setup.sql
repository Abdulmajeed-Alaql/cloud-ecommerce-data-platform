-- Databricks notebook source
SELECT
    current_catalog() AS current_catalog,
    current_schema() AS current_schema;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.ecommerce_bronze;
CREATE SCHEMA IF NOT EXISTS workspace.ecommerce_silver;
CREATE SCHEMA IF NOT EXISTS workspace.ecommerce_gold;


SHOW SCHEMAS IN workspace;

-- COMMAND ----------

 show schemas;

-- COMMAND ----------

SELECT
    order_id,
    order_status,
    total_amount,
    tax_amount,
    amount_with_tax
FROM workspace.ecommerce_bronze.environment_test_orders
ORDER BY amount_with_tax DESC;

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS
workspace.ecommerce_bronze.raw_files;

-- COMMAND ----------

show volumes in workspace.ecommerce_bronze

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS
workspace.ecommerce_dbt;

-- COMMAND ----------

SHOW SCHEMAS IN workspace;