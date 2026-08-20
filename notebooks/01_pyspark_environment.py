# Databricks notebook source
print(f"Spark version: {spark.version}")


# COMMAND ----------

orders_data = [
    (1001, "completed", 250.75),
    (1002, "shipped", 120.50),
    (1003, "cancelled", 80.00),
    (1004, "completed", 430.25),
]

columns = [
    "order_id",
    "order_status",
    "total_amount",
]

orders_df = spark.createDataFrame(
    orders_data,
    columns,
)

display(orders_df)

# COMMAND ----------

from pyspark.sql import functions as F

completed_orders_df = (
    orders_df
    .filter(F.col("order_status") == "completed")
    .withColumn(
        "tax_amount",
        F.round(F.col("total_amount") * 0.15, 2),
    )
    .withColumn(
        "amount_with_tax",
        F.round(
            F.col("total_amount") + F.col("tax_amount"),
            2,
        ),
    )
)

display(completed_orders_df)

# COMMAND ----------

(
    completed_orders_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.ecommerce_bronze.environment_test_orders"
    )
)