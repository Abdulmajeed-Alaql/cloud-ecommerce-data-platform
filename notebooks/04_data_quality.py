# Databricks notebook source
CATALOG = "workspace"
BRONZE_SCHEMA = "ecommerce_bronze"
SILVER_SCHEMA = "ecommerce_silver"
MONITORING_SCHEMA = "ecommerce_monitoring"

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS
    {CATALOG}.{MONITORING_SCHEMA}
    """
)

print(
    f"Monitoring schema is ready: "
    f"{CATALOG}.{MONITORING_SCHEMA}"
)

# COMMAND ----------

from datetime import datetime, timezone
from functools import reduce
import operator
import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


RUN_ID = str(uuid.uuid4())

CHECKED_AT = (
    datetime.now(timezone.utc)
    .replace(tzinfo=None)
)

quality_results = []


def silver_table(table_name: str) -> str:
    return (
        f"{CATALOG}."
        f"{SILVER_SCHEMA}."
        f"{table_name}"
    )


def bronze_table(table_name: str) -> str:
    return (
        f"{CATALOG}."
        f"{BRONZE_SCHEMA}."
        f"{table_name}"
    )


print(f"Data quality run ID: {RUN_ID}")

# COMMAND ----------

def record_result(
    table_name: str,
    check_name: str,
    check_type: str,
    total_rows: int,
    failed_rows: int,
    severity: str = "CRITICAL",
    allowed_failures: int = 0,
    details: str = "",
) -> None:

    status = (
        "PASS"
        if failed_rows <= allowed_failures
        else "FAIL"
    )

    failure_rate = (
        failed_rows / total_rows
        if total_rows > 0
        else 0.0
    )

    quality_results.append(
        (
            RUN_ID,
            CHECKED_AT,
            "silver",
            table_name,
            check_name,
            check_type,
            severity,
            status,
            int(total_rows),
            int(failed_rows),
            float(failure_rate),
            details,
        )
    )

    symbol = "✅" if status == "PASS" else "❌"

    print(
        f"{symbol} {table_name} | "
        f"{check_name} | "
        f"failed rows: {failed_rows:,}"
    )

# COMMAND ----------

def check_not_null(
    table_name: str,
    columns: list[str],
    severity: str = "CRITICAL",
) -> None:
    dataframe = spark.table(
        silver_table(table_name)
    )

    total_rows = dataframe.count()

    null_condition = reduce(
        operator.or_,
        [
            F.col(column_name).isNull()
            for column_name in columns
        ],
    )

    failed_rows = (
        dataframe
        .filter(null_condition)
        .count()
    )

    record_result(
        table_name=table_name,
        check_name=(
            f"not_null: {', '.join(columns)}"
        ),
        check_type="not_null",
        total_rows=total_rows,
        failed_rows=failed_rows,
        severity=severity,
        details=(
            "Required columns must not contain NULL."
        ),
    )


def check_unique(
    table_name: str,
    columns: list[str],
    severity: str = "CRITICAL",
) -> None:
    dataframe = spark.table(
        silver_table(table_name)
    )

    total_rows = dataframe.count()

    unique_rows = (
        dataframe
        .dropDuplicates(columns)
        .count()
    )

    failed_rows = total_rows - unique_rows

    record_result(
        table_name=table_name,
        check_name=(
            f"unique: {', '.join(columns)}"
        ),
        check_type="uniqueness",
        total_rows=total_rows,
        failed_rows=failed_rows,
        severity=severity,
        details=(
            "Business-key columns must be unique."
        ),
    )


def check_foreign_key(
    child_table: str,
    parent_table: str,
    key_columns: list[str],
    severity: str = "CRITICAL",
) -> None:
    child_df = spark.table(
        silver_table(child_table)
    )

    parent_keys_df = (
        spark.table(
            silver_table(parent_table)
        )
        .select(*key_columns)
        .dropDuplicates()
    )

    total_rows = child_df.count()

    non_null_condition = reduce(
        operator.and_,
        [
            F.col(column_name).isNotNull()
            for column_name in key_columns
        ],
    )

    failed_rows = (
        child_df
        .filter(non_null_condition)
        .join(
            parent_keys_df,
            on=key_columns,
            how="left_anti",
        )
        .count()
    )

    record_result(
        table_name=child_table,
        check_name=(
            f"foreign_key: "
            f"{child_table} → {parent_table}"
        ),
        check_type="referential_integrity",
        total_rows=total_rows,
        failed_rows=failed_rows,
        severity=severity,
        details=(
            f"Keys {key_columns} must exist "
            f"in {parent_table}."
        ),
    )


def check_condition(
    table_name: str,
    check_name: str,
    valid_condition,
    severity: str = "CRITICAL",
    details: str = "",
) -> None:
    dataframe = spark.table(
        silver_table(table_name)
    )

    total_rows = dataframe.count()

    valid_condition_with_nulls = F.coalesce(
        valid_condition,
        F.lit(False),
    )

    failed_rows = (
        dataframe
        .filter(
            ~valid_condition_with_nulls
        )
        .count()
    )

    record_result(
        table_name=table_name,
        check_name=check_name,
        check_type="business_rule",
        total_rows=total_rows,
        failed_rows=failed_rows,
        severity=severity,
        details=details,
    )

# COMMAND ----------

key_rules = {
    "customers": {
        "required": [
            "customer_id",
            "customer_unique_id",
        ],
        "unique": [
            "customer_id",
        ],
    },
    "orders": {
        "required": [
            "order_id",
            "customer_id",
        ],
        "unique": [
            "order_id",
        ],
    },
    "order_items": {
        "required": [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
        ],
        "unique": [
            "order_id",
            "order_item_id",
        ],
    },
    "payments": {
        "required": [
            "order_id",
            "payment_sequential",
            "payment_value",
        ],
        "unique": [
            "order_id",
            "payment_sequential",
        ],
    },
    "reviews": {
        "required": [
            "review_id",
            "order_id",
            "review_score",
        ],
        "unique": [
            "review_id",
        ],
    },
    "products": {
        "required": [
            "product_id",
        ],
        "unique": [
            "product_id",
        ],
    },
    "sellers": {
        "required": [
            "seller_id",
        ],
        "unique": [
            "seller_id",
        ],
    },
}


for table_name, rules in key_rules.items():
    check_not_null(
        table_name=table_name,
        columns=rules["required"],
    )

    check_unique(
        table_name=table_name,
        columns=rules["unique"],
    )

# COMMAND ----------

check_foreign_key(
    child_table="orders",
    parent_table="customers",
    key_columns=["customer_id"],
)

check_foreign_key(
    child_table="order_items",
    parent_table="orders",
    key_columns=["order_id"],
)

check_foreign_key(
    child_table="order_items",
    parent_table="products",
    key_columns=["product_id"],
)

check_foreign_key(
    child_table="order_items",
    parent_table="sellers",
    key_columns=["seller_id"],
)

check_foreign_key(
    child_table="payments",
    parent_table="orders",
    key_columns=["order_id"],
)

check_foreign_key(
    child_table="reviews",
    parent_table="orders",
    key_columns=["order_id"],
)

# COMMAND ----------

check_condition(
    table_name="order_items",
    check_name="non_negative_item_amounts",
    valid_condition=(
        (F.col("price") >= 0)
        &
        (F.col("freight_value") >= 0)
        &
        (F.col("item_total_amount") >= 0)
    ),
    details=(
        "Price, freight, and item total "
        "must not be negative."
    ),
)


check_condition(
    table_name="payments",
    check_name="non_negative_payment_value",
    valid_condition=(
        F.col("payment_value") >= 0
    ),
    details=(
        "Payment value must not be negative."
    ),
)


check_condition(
    table_name="reviews",
    check_name="valid_review_score",
    valid_condition=(
        F.col("review_score").between(
            1,
            5,
        )
    ),
    details=(
        "Review score must be between 1 and 5."
    ),
)


check_condition(
    table_name="orders",
    check_name="valid_delivery_sequence",
    valid_condition=(
        F.col(
            "order_delivered_customer_date"
        ).isNull()
        |
        (
            F.col(
                "order_delivered_customer_date"
            )
            >=
            F.col(
                "order_purchase_timestamp"
            )
        )
    ),
    details=(
        "Delivery date must not occur "
        "before purchase date."
    ),
)

# COMMAND ----------

reviews_raw_df = spark.table(
    bronze_table("reviews_raw")
)

reviews_raw_count = reviews_raw_df.count()

reviews_unique_count = (
    reviews_raw_df
    .dropDuplicates(["review_id"])
    .count()
)

duplicate_review_rows = (
    reviews_raw_count
    - reviews_unique_count
)

record_result(
    table_name="reviews_raw",
    check_name="duplicate_review_ids_in_source",
    check_type="source_quality",
    total_rows=reviews_raw_count,
    failed_rows=duplicate_review_rows,
    severity="WARNING",
    allowed_failures=duplicate_review_rows,
    details=(
        "The source contains repeated review_id "
        "values. Silver currently retains one "
        "record per review_id."
    ),
)

# COMMAND ----------

quality_schema = T.StructType(
    [
        T.StructField(
            "run_id",
            T.StringType(),
            False,
        ),
        T.StructField(
            "checked_at",
            T.TimestampType(),
            False,
        ),
        T.StructField(
            "layer",
            T.StringType(),
            False,
        ),
        T.StructField(
            "table_name",
            T.StringType(),
            False,
        ),
        T.StructField(
            "check_name",
            T.StringType(),
            False,
        ),
        T.StructField(
            "check_type",
            T.StringType(),
            False,
        ),
        T.StructField(
            "severity",
            T.StringType(),
            False,
        ),
        T.StructField(
            "status",
            T.StringType(),
            False,
        ),
        T.StructField(
            "total_rows",
            T.LongType(),
            False,
        ),
        T.StructField(
            "failed_rows",
            T.LongType(),
            False,
        ),
        T.StructField(
            "failure_rate",
            T.DoubleType(),
            False,
        ),
        T.StructField(
            "details",
            T.StringType(),
            False,
        ),
    ]
)


quality_results_df = spark.createDataFrame(
    quality_results,
    schema=quality_schema,
)


QUALITY_RESULTS_TABLE = (
    f"{CATALOG}."
    f"{MONITORING_SCHEMA}."
    f"data_quality_results"
)


(
    quality_results_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(QUALITY_RESULTS_TABLE)
)


display(
    quality_results_df.orderBy(
        "status",
        "table_name",
        "check_name",
    )
)

# COMMAND ----------

critical_failures_df = (
    quality_results_df
    .filter(
        (F.col("severity") == "CRITICAL")
        &
        (F.col("status") == "FAIL")
    )
)

critical_failure_count = (
    critical_failures_df.count()
)

if critical_failure_count > 0:
    display(critical_failures_df)

    raise ValueError(
        f"Data quality gate failed: "
        f"{critical_failure_count} "
        f"critical checks failed."
    )

print(
    "✅ Data quality gate passed. "
    "No critical checks failed."
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     severity,
# MAGIC     status,
# MAGIC     COUNT(*) AS check_count,
# MAGIC     SUM(failed_rows) AS failed_rows
# MAGIC FROM workspace.ecommerce_monitoring.data_quality_results
# MAGIC WHERE checked_at = (
# MAGIC     SELECT MAX(checked_at)
# MAGIC     FROM workspace.ecommerce_monitoring.data_quality_results
# MAGIC )
# MAGIC GROUP BY
# MAGIC     severity,
# MAGIC     status
# MAGIC ORDER BY
# MAGIC     severity,
# MAGIC     status;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     check_name,
# MAGIC     severity,
# MAGIC     failed_rows,
# MAGIC     failure_rate,
# MAGIC     details
# MAGIC FROM workspace.ecommerce_monitoring.data_quality_results
# MAGIC WHERE checked_at = (
# MAGIC     SELECT MAX(checked_at)
# MAGIC     FROM workspace.ecommerce_monitoring.data_quality_results
# MAGIC )
# MAGIC AND status = 'FAIL'
# MAGIC ORDER BY
# MAGIC     severity,
# MAGIC     table_name;