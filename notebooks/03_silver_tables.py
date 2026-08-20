# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


CATALOG = "workspace"
BRONZE_SCHEMA = "ecommerce_bronze"
SILVER_SCHEMA = "ecommerce_silver"


def bronze_table(table_name: str) -> str:
    """Return the full name of a Bronze table."""

    return (
        f"{CATALOG}."
        f"{BRONZE_SCHEMA}."
        f"{table_name}"
    )


def silver_table(table_name: str) -> str:
    """Return the full name of a Silver table."""

    return (
        f"{CATALOG}."
        f"{SILVER_SCHEMA}."
        f"{table_name}"
    )

# COMMAND ----------

def write_silver_table(
    dataframe: DataFrame,
    table_name: str,
) -> int:
    """
    Save a DataFrame as a Silver Delta table
    and return its final row count.
    """

    full_table_name = silver_table(table_name)

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(full_table_name)
    )

    final_row_count = spark.table(
        full_table_name
    ).count()

    print(
        f"Created {full_table_name}: "
        f"{final_row_count:,} rows"
    )

    return final_row_count

# COMMAND ----------

customers_raw_df = spark.table(
    bronze_table("customers_raw")
)

display(customers_raw_df.limit(10))

# COMMAND ----------

customers_df = (
    customers_raw_df

    .select(
        F.trim(
            F.col("customer_id")
        ).alias("customer_id"),

        F.trim(
            F.col("customer_unique_id")
        ).alias("customer_unique_id"),

        F.col(
            "customer_zip_code_prefix"
        ).cast("integer").alias(
            "customer_zip_code_prefix"
        ),

        F.lower(
            F.trim(
                F.col("customer_city")
            )
        ).alias("customer_city"),

        F.upper(
            F.trim(
                F.col("customer_state")
            )
        ).alias("customer_state"),

        F.col("_source_file"),
        F.col("_ingested_at"),
    )

    .filter(
        F.col("customer_id").isNotNull()
    )

    .filter(
        F.length(
            F.col("customer_id")
        ) > 0
    )

    .dropDuplicates(
        ["customer_id"]
    )

    .withColumn(
        "_silver_processed_at",
        F.current_timestamp(),
    )
)

display(customers_df.limit(10))

# COMMAND ----------

customers_raw_count = customers_raw_df.count()
customers_silver_count = customers_df.count()

null_customer_ids = (
    customers_df
    .filter(
        F.col("customer_id").isNull()
    )
    .count()
)

duplicate_customer_ids = (
    customers_df
    .groupBy("customer_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


print(
    f"Raw customers: "
    f"{customers_raw_count:,}"
)

print(
    f"Silver customers: "
    f"{customers_silver_count:,}"
)

print(
    f"Null customer IDs: "
    f"{null_customer_ids}"
)

print(
    f"Duplicate customer IDs: "
    f"{duplicate_customer_ids}"
)

# COMMAND ----------

write_silver_table(
    dataframe=customers_df,
    table_name="customers",
)

# COMMAND ----------

orders_raw_df = spark.table(
    bronze_table("orders_raw")
)

display(orders_raw_df.limit(10))

# COMMAND ----------

orders_df = (
    orders_raw_df

    .select(
        F.trim(
            F.col("order_id")
        ).alias("order_id"),

        F.trim(
            F.col("customer_id")
        ).alias("customer_id"),

        F.lower(
            F.trim(
                F.col("order_status")
            )
        ).alias("order_status"),

        F.to_timestamp(
            F.col("order_purchase_timestamp")
        ).alias(
            "order_purchase_timestamp"
        ),

        F.to_timestamp(
            F.col("order_approved_at")
        ).alias(
            "order_approved_at"
        ),

        F.to_timestamp(
            F.col("order_delivered_carrier_date")
        ).alias(
            "order_delivered_carrier_date"
        ),

        F.to_timestamp(
            F.col("order_delivered_customer_date")
        ).alias(
            "order_delivered_customer_date"
        ),

        F.to_timestamp(
            F.col("order_estimated_delivery_date")
        ).alias(
            "order_estimated_delivery_date"
        ),

        F.col("_source_file"),
        F.col("_ingested_at"),
    )

    .filter(
        F.col("order_id").isNotNull()
    )

    .filter(
        F.length(
            F.col("order_id")
        ) > 0
    )

    .dropDuplicates(
        ["order_id"]
    )

    .withColumn(
        "delivery_days",
        F.datediff(
            F.col(
                "order_delivered_customer_date"
            ),
            F.col(
                "order_purchase_timestamp"
            ),
        ),
    )

    .withColumn(
        "is_delivered_late",
        F.when(
            F.col(
                "order_delivered_customer_date"
            ).isNull()
            |
            F.col(
                "order_estimated_delivery_date"
            ).isNull(),
            F.lit(None).cast("boolean"),
        )
        .when(
            F.col(
                "order_delivered_customer_date"
            )
            >
            F.col(
                "order_estimated_delivery_date"
            ),
            F.lit(True),
        )
        .otherwise(
            F.lit(False)
        ),
    )

    .withColumn(
        "purchase_date",
        F.to_date(
            F.col("order_purchase_timestamp")
        ),
    )

    .withColumn(
        "_silver_processed_at",
        F.current_timestamp(),
    )
)

display(orders_df.limit(10))

# COMMAND ----------

orders_raw_count = orders_raw_df.count()
orders_silver_count = orders_df.count()

null_order_ids = (
    orders_df
    .filter(
        F.col("order_id").isNull()
    )
    .count()
)

duplicate_order_ids = (
    orders_df
    .groupBy("order_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

invalid_delivery_days = (
    orders_df
    .filter(
        F.col("delivery_days") < 0
    )
    .count()
)


print(
    f"Raw orders: "
    f"{orders_raw_count:,}"
)

print(
    f"Silver orders: "
    f"{orders_silver_count:,}"
)

print(
    f"Null order IDs: "
    f"{null_order_ids}"
)

print(
    f"Duplicate order IDs: "
    f"{duplicate_order_ids}"
)

print(
    f"Negative delivery days: "
    f"{invalid_delivery_days}"
)

# COMMAND ----------

orphan_orders_df = (
    orders_df.alias("orders")
    .join(
        customers_df.alias("customers"),
        on="customer_id",
        how="left_anti",
    )
)

orphan_orders_count = orphan_orders_df.count()

print(
    f"Orders without matching customers: "
    f"{orphan_orders_count}"
)

# COMMAND ----------

write_silver_table(
    dataframe=orders_df,
    table_name="orders",
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     'customers' AS table_name,
# MAGIC     COUNT(*) AS row_count
# MAGIC FROM workspace.ecommerce_silver.customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'orders',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_silver.orders;

# COMMAND ----------

order_items_raw_df = spark.table(
    bronze_table("order_items_raw")
)

display(order_items_raw_df.limit(10))

# COMMAND ----------

order_items_df = (
    order_items_raw_df
    .select(
        F.trim(
            F.col("order_id")
        ).alias("order_id"),

        F.col(
            "order_item_id"
        ).cast("integer").alias(
            "order_item_id"
        ),

        F.trim(
            F.col("product_id")
        ).alias("product_id"),

        F.trim(
            F.col("seller_id")
        ).alias("seller_id"),

        F.to_timestamp(
            F.col("shipping_limit_date")
        ).alias("shipping_limit_date"),

        F.col("price")
        .cast("decimal(12,2)")
        .alias("price"),

        F.col("freight_value")
        .cast("decimal(12,2)")
        .alias("freight_value"),

        F.col("_source_file"),
        F.col("_ingested_at"),
    )
    .filter(
        F.col("order_id").isNotNull()
    )
    .filter(
        F.col("order_item_id").isNotNull()
    )
    .filter(
        F.col("product_id").isNotNull()
    )
    .filter(
        F.col("seller_id").isNotNull()
    )
    .filter(
        F.col("price") >= 0
    )
    .filter(
        F.col("freight_value") >= 0
    )
    .dropDuplicates(
        [
            "order_id",
            "order_item_id",
        ]
    )
    .withColumn(
        "item_total_amount",
        F.round(
            F.col("price")
            + F.col("freight_value"),
            2,
        ),
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp(),
    )
)

display(order_items_df.limit(10))

# COMMAND ----------

order_items_raw_count = (
    order_items_raw_df.count()
)

order_items_silver_count = (
    order_items_df.count()
)

duplicate_order_items = (
    order_items_df
    .groupBy(
        "order_id",
        "order_item_id",
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

orphan_order_items = (
    order_items_df.alias("items")
    .join(
        orders_df.alias("orders"),
        on="order_id",
        how="left_anti",
    )
    .count()
)

print(
    f"Raw order items: "
    f"{order_items_raw_count:,}"
)

print(
    f"Silver order items: "
    f"{order_items_silver_count:,}"
)

print(
    f"Duplicate order items: "
    f"{duplicate_order_items}"
)

print(
    f"Order items without matching orders: "
    f"{orphan_order_items}"
)

# COMMAND ----------

write_silver_table(
    dataframe=order_items_df,
    table_name="order_items",
)

# COMMAND ----------

payments_raw_df = spark.table(
    bronze_table("payments_raw")
)

display(payments_raw_df.limit(10))

# COMMAND ----------

payments_df = (
    payments_raw_df
    .select(
        F.trim(
            F.col("order_id")
        ).alias("order_id"),

        F.col(
            "payment_sequential"
        ).cast("integer").alias(
            "payment_sequential"
        ),

        F.lower(
            F.trim(
                F.col("payment_type")
            )
        ).alias("payment_type"),

        F.col(
            "payment_installments"
        ).cast("integer").alias(
            "payment_installments"
        ),

        F.col("payment_value")
        .cast("decimal(12,2)")
        .alias("payment_value"),

        F.col("_source_file"),
        F.col("_ingested_at"),
    )
    .filter(
        F.col("order_id").isNotNull()
    )
    .filter(
        F.col(
            "payment_sequential"
        ).isNotNull()
    )
    .filter(
        F.col("payment_value") >= 0
    )
    .dropDuplicates(
        [
            "order_id",
            "payment_sequential",
        ]
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp(),
    )
)

display(payments_df.limit(10))

# COMMAND ----------

payments_raw_count = (
    payments_raw_df.count()
)

payments_silver_count = (
    payments_df.count()
)

duplicate_payments = (
    payments_df
    .groupBy(
        "order_id",
        "payment_sequential",
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

orphan_payments = (
    payments_df.alias("payments")
    .join(
        orders_df.alias("orders"),
        on="order_id",
        how="left_anti",
    )
    .count()
)

print(
    f"Raw payments: "
    f"{payments_raw_count:,}"
)

print(
    f"Silver payments: "
    f"{payments_silver_count:,}"
)

print(
    f"Duplicate payments: "
    f"{duplicate_payments}"
)

print(
    f"Payments without matching orders: "
    f"{orphan_payments}"
)

# COMMAND ----------

write_silver_table(
    dataframe=payments_df,
    table_name="payments",
)

# COMMAND ----------

reviews_raw_df = spark.table(
    bronze_table("reviews_raw")
)

display(reviews_raw_df.limit(10))

# COMMAND ----------

reviews_df = (
    reviews_raw_df
    .select(
        F.trim(
            F.col("review_id")
        ).alias("review_id"),

        F.trim(
            F.col("order_id")
        ).alias("order_id"),

        F.col("review_score")
        .cast("integer")
        .alias("review_score"),

        F.trim(
            F.col(
                "review_comment_title"
            )
        ).alias(
            "review_comment_title"
        ),

        F.trim(
            F.col(
                "review_comment_message"
            )
        ).alias(
            "review_comment_message"
        ),

        F.to_timestamp(
            F.col(
                "review_creation_date"
            )
        ).alias(
            "review_creation_date"
        ),

        F.to_timestamp(
            F.col(
                "review_answer_timestamp"
            )
        ).alias(
            "review_answer_timestamp"
        ),

        F.col("_source_file"),
        F.col("_ingested_at"),
    )
    .filter(
        F.col("review_id").isNotNull()
    )
    .filter(
        F.col("order_id").isNotNull()
    )
    .filter(
        F.col("review_score").between(
            1,
            5,
        )
    )
    .dropDuplicates(
        ["review_id"]
    )
    .withColumn(
        "has_comment",
        F.when(
            F.col(
                "review_comment_message"
            ).isNotNull()
            & (
                F.length(
                    F.col(
                        "review_comment_message"
                    )
                ) > 0
            ),
            F.lit(True),
        ).otherwise(
            F.lit(False)
        ),
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp(),
    )
)

display(reviews_df.limit(10))

# COMMAND ----------

reviews_raw_count = reviews_raw_df.count()
reviews_silver_count = reviews_df.count()

duplicate_reviews = (
    reviews_df
    .groupBy("review_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

orphan_reviews = (
    reviews_df.alias("reviews")
    .join(
        orders_df.alias("orders"),
        on="order_id",
        how="left_anti",
    )
    .count()
)

invalid_review_scores = (
    reviews_df
    .filter(
        ~F.col("review_score").between(
            1,
            5,
        )
    )
    .count()
)

print(
    f"Raw reviews: "
    f"{reviews_raw_count:,}"
)

print(
    f"Silver reviews: "
    f"{reviews_silver_count:,}"
)

print(
    f"Duplicate reviews: "
    f"{duplicate_reviews}"
)

print(
    f"Reviews without matching orders: "
    f"{orphan_reviews}"
)

print(
    f"Invalid review scores: "
    f"{invalid_review_scores}"
)

# COMMAND ----------

write_silver_table(
    dataframe=reviews_df,
    table_name="reviews",
)

# COMMAND ----------

products_raw_df = spark.table(
    bronze_table("products_raw")
)

display(products_raw_df.limit(10))

# COMMAND ----------

products_df = (
    products_raw_df
    .select(
        F.trim(
            F.col("product_id")
        ).alias("product_id"),

        F.lower(
            F.trim(
                F.col(
                    "product_category_name"
                )
            )
        ).alias(
            "product_category_name"
        ),

        F.col(
            "product_name_lenght"
        ).cast("integer").alias(
            "product_name_length"
        ),

        F.col(
            "product_description_lenght"
        ).cast("integer").alias(
            "product_description_length"
        ),

        F.col(
            "product_photos_qty"
        ).cast("integer").alias(
            "product_photos_qty"
        ),

        F.col(
            "product_weight_g"
        ).cast("integer").alias(
            "product_weight_g"
        ),

        F.col(
            "product_length_cm"
        ).cast("integer").alias(
            "product_length_cm"
        ),

        F.col(
            "product_height_cm"
        ).cast("integer").alias(
            "product_height_cm"
        ),

        F.col(
            "product_width_cm"
        ).cast("integer").alias(
            "product_width_cm"
        ),

        F.col("_source_file"),
        F.col("_ingested_at"),
    )
    .filter(
        F.col("product_id").isNotNull()
    )
    .dropDuplicates(
        ["product_id"]
    )
    .withColumn(
        "product_volume_cm3",
        F.col("product_length_cm")
        * F.col("product_height_cm")
        * F.col("product_width_cm"),
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp(),
    )
)

display(products_df.limit(10))


# COMMAND ----------

products_raw_count = (
    products_raw_df.count()
)

products_silver_count = (
    products_df.count()
)

duplicate_products = (
    products_df
    .groupBy("product_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(
    f"Raw products: "
    f"{products_raw_count:,}"
)

print(
    f"Silver products: "
    f"{products_silver_count:,}"
)

print(
    f"Duplicate products: "
    f"{duplicate_products}"
)

# COMMAND ----------

write_silver_table(
    dataframe=products_df,
    table_name="products",
)

# COMMAND ----------

sellers_raw_df = spark.table(
    bronze_table("sellers_raw")
)

display(sellers_raw_df.limit(10))

# COMMAND ----------

sellers_df = (
    sellers_raw_df
    .select(
        F.trim(
            F.col("seller_id")
        ).alias("seller_id"),

        F.col(
            "seller_zip_code_prefix"
        ).cast("integer").alias(
            "seller_zip_code_prefix"
        ),

        F.lower(
            F.trim(
                F.col("seller_city")
            )
        ).alias("seller_city"),

        F.upper(
            F.trim(
                F.col("seller_state")
            )
        ).alias("seller_state"),

        F.col("_source_file"),
        F.col("_ingested_at"),
    )
    .filter(
        F.col("seller_id").isNotNull()
    )
    .filter(
        F.length(
            F.col("seller_id")
        ) > 0
    )
    .dropDuplicates(
        ["seller_id"]
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp(),
    )
)

display(sellers_df.limit(10))

# COMMAND ----------

sellers_raw_count = sellers_raw_df.count()
sellers_silver_count = sellers_df.count()

duplicate_sellers = (
    sellers_df
    .groupBy("seller_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(
    f"Raw sellers: "
    f"{sellers_raw_count:,}"
)

print(
    f"Silver sellers: "
    f"{sellers_silver_count:,}"
)

print(
    f"Duplicate sellers: "
    f"{duplicate_sellers}"
)

# COMMAND ----------

write_silver_table(
    dataframe=sellers_df,
    table_name="sellers",
)

# COMMAND ----------

orphan_products = (
    order_items_df.alias("items")
    .join(
        products_df.alias("products"),
        on="product_id",
        how="left_anti",
    )
    .count()
)

orphan_sellers = (
    order_items_df.alias("items")
    .join(
        sellers_df.alias("sellers"),
        on="seller_id",
        how="left_anti",
    )
    .count()
)

print(
    f"Order items without matching products: "
    f"{orphan_products}"
)

print(
    f"Order items without matching sellers: "
    f"{orphan_sellers}"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     'customers' AS table_name,
# MAGIC     COUNT(*) AS row_count
# MAGIC FROM workspace.ecommerce_silver.customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'orders',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_silver.orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'order_items',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_silver.order_items
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'payments',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_silver.payments
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'reviews',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_silver.reviews
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'products',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_silver.products
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'sellers',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_silver.sellers
# MAGIC
# MAGIC ORDER BY table_name;