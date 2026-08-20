# Databricks notebook source
CATALOG = "workspace"
BRONZE_SCHEMA = "ecommerce_bronze"
RAW_VOLUME = "raw_files"

RAW_DATA_PATH = (
    f"/Volumes/{CATALOG}/"
    f"{BRONZE_SCHEMA}/"
    f"{RAW_VOLUME}"
)

print(f"Raw data path: {RAW_DATA_PATH}")

# COMMAND ----------

raw_files = dbutils.fs.ls(RAW_DATA_PATH)

display(raw_files)

# COMMAND ----------

DATASETS = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
}

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def read_raw_csv(file_name: str) -> DataFrame:

    file_path = f"{RAW_DATA_PATH}/{file_name}"

    raw_dataframe = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(file_path)
    )

    dataframe = (
        raw_dataframe
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
        )
        .withColumn(
            "_ingested_at",
            F.current_timestamp(),
        )
    )

    return dataframe

# COMMAND ----------

customers_df = read_raw_csv(
    DATASETS["customers"]
)

display(customers_df)

# COMMAND ----------

customers_df.printSchema()

# COMMAND ----------

customers_count = customers_df.count()

print(f"Customers row count: {customers_count}")

# COMMAND ----------

CUSTOMERS_TABLE = (
    f"{CATALOG}."
    f"{BRONZE_SCHEMA}."
    f"customers_raw"
)

(
    customers_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(CUSTOMERS_TABLE)
)

print(
    f"Created table: {CUSTOMERS_TABLE}"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_unique_id,
# MAGIC     customer_city,
# MAGIC     customer_state,
# MAGIC     _source_file,
# MAGIC     _ingested_at
# MAGIC FROM workspace.ecommerce_bronze.customers_raw
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS customer_count
# MAGIC FROM workspace.ecommerce_bronze.customers_raw;