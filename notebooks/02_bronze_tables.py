# Databricks notebook source
CATALOG = "workspace"
BRONZE_SCHEMA = "ecommerce_bronze"
RAW_VOLUME = "raw_files"

RAW_DATA_PATH = (
    f"/Volumes/{CATALOG}/"
    f"{BRONZE_SCHEMA}/"
    f"{RAW_VOLUME}"
)

DATASETS = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
}

print(f"Raw data path: {RAW_DATA_PATH}")
print(f"Number of datasets: {len(DATASETS)}")

# COMMAND ----------

available_files = {
    file_info.name
    for file_info in dbutils.fs.ls(RAW_DATA_PATH)
}

missing_files = [
    file_name
    for file_name in DATASETS.values()
    if file_name not in available_files
]

if missing_files:
    raise FileNotFoundError(
        f"Missing source files: {missing_files}"
    )

print("All required CSV files are available.")

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
            F.col("_metadata.file_path").alias(
                "_source_file"
            ),
        )
        .withColumn(
            "_ingested_at",
            F.current_timestamp(),
        )
    )

    return dataframe

# COMMAND ----------

def write_bronze_table(
    dataset_name: str,
    file_name: str,
) -> tuple:

    table_name = (
        f"{CATALOG}."
        f"{BRONZE_SCHEMA}."
        f"{dataset_name}_raw"
    )

    dataframe = read_raw_csv(file_name)

    row_count = dataframe.count()
    column_count = len(dataframe.columns)

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(table_name)
    )

    print(
        f"Created {table_name}: "
        f"{row_count:,} rows, "
        f"{column_count} columns"
    )

    return (
        dataset_name,
        file_name,
        table_name,
        row_count,
        column_count,
        "success",
    )

# COMMAND ----------

ingestion_results = []

for dataset_name, file_name in DATASETS.items():
    print(f"Processing dataset: {dataset_name}")

    result = write_bronze_table(
        dataset_name=dataset_name,
        file_name=file_name,
    )

    ingestion_results.append(result)

print(
    f"Completed ingestion for "
    f"{len(ingestion_results)} datasets."
)

# COMMAND ----------

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


if not ingestion_results:
    raise ValueError(
        "No ingestion results were created. "
        "Run the ingestion loop first."
    )


summary_schema = StructType(
    [
        StructField(
            "dataset_name",
            StringType(),
            False,
        ),
        StructField(
            "source_file",
            StringType(),
            False,
        ),
        StructField(
            "table_name",
            StringType(),
            False,
        ),
        StructField(
            "row_count",
            LongType(),
            False,
        ),
        StructField(
            "column_count",
            IntegerType(),
            False,
        ),
        StructField(
            "status",
            StringType(),
            False,
        ),
    ]
)


ingestion_summary_df = spark.createDataFrame(
    ingestion_results,
    schema=summary_schema,
)

display(
    ingestion_summary_df.orderBy(
        "dataset_name"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN workspace.ecommerce_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     'customers_raw' AS table_name,
# MAGIC     COUNT(*) AS row_count
# MAGIC FROM workspace.ecommerce_bronze.customers_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'orders_raw',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_bronze.orders_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'order_items_raw',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_bronze.order_items_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'payments_raw',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_bronze.payments_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'reviews_raw',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_bronze.reviews_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'products_raw',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_bronze.products_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'sellers_raw',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_bronze.sellers_raw
# MAGIC
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS
# MAGIC workspace.ecommerce_bronze.environment_test_orders;