# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


CATALOG = "workspace"
SILVER_SCHEMA = "ecommerce_silver"
GOLD_SCHEMA = "ecommerce_gold"


def silver_table(table_name: str) -> str:
    return (
        f"{CATALOG}."
        f"{SILVER_SCHEMA}."
        f"{table_name}"
    )


def gold_table(table_name: str) -> str:
    return (
        f"{CATALOG}."
        f"{GOLD_SCHEMA}."
        f"{table_name}"
    )


def write_gold_table(
    dataframe: DataFrame,
    table_name: str,
) -> int:
    full_table_name = gold_table(table_name)

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(full_table_name)
    )

    row_count = spark.table(
        full_table_name
    ).count()

    print(
        f"Created {full_table_name}: "
        f"{row_count:,} rows"
    )

    return row_count


print("Gold environment is ready.")

# COMMAND ----------

customers_df = spark.table(
    silver_table("customers")
)

orders_df = spark.table(
    silver_table("orders")
)

order_items_df = spark.table(
    silver_table("order_items")
)

payments_df = spark.table(
    silver_table("payments")
)

reviews_df = spark.table(
    silver_table("reviews")
)

products_df = spark.table(
    silver_table("products")
)

sellers_df = spark.table(
    silver_table("sellers")
)


print("Silver tables loaded successfully.")

# COMMAND ----------

dim_customers_df = (
    customers_df
    .select(
        F.sha2(
            F.col("customer_id"),
            256,
        ).alias("customer_key"),

        F.col("customer_id"),
        F.col("customer_unique_id"),
        F.col("customer_zip_code_prefix"),
        F.col("customer_city"),
        F.col("customer_state"),

        F.current_timestamp().alias(
            "_gold_processed_at"
        ),
    )
    .dropDuplicates(
        ["customer_id"]
    )
)

display(dim_customers_df.limit(10))

# COMMAND ----------

dim_products_df = (
    products_df
    .select(
        F.sha2(
            F.col("product_id"),
            256,
        ).alias("product_key"),

        F.col("product_id"),

        F.coalesce(
            F.col("product_category_name"),
            F.lit("unknown"),
        ).alias("product_category_name"),

        F.col("product_name_length"),
        F.col("product_description_length"),
        F.col("product_photos_qty"),
        F.col("product_weight_g"),
        F.col("product_length_cm"),
        F.col("product_height_cm"),
        F.col("product_width_cm"),
        F.col("product_volume_cm3"),

        F.current_timestamp().alias(
            "_gold_processed_at"
        ),
    )
    .dropDuplicates(
        ["product_id"]
    )
)

display(dim_products_df.limit(10))

# COMMAND ----------

dim_sellers_df = (
    sellers_df
    .select(
        F.sha2(
            F.col("seller_id"),
            256,
        ).alias("seller_key"),

        F.col("seller_id"),
        F.col("seller_zip_code_prefix"),
        F.col("seller_city"),
        F.col("seller_state"),

        F.current_timestamp().alias(
            "_gold_processed_at"
        ),
    )
    .dropDuplicates(
        ["seller_id"]
    )
)

display(dim_sellers_df.limit(10))

# COMMAND ----------

dim_date_df = (
    orders_df
    .select(
        F.col("purchase_date").alias(
            "full_date"
        )
    )
    .filter(
        F.col("full_date").isNotNull()
    )
    .dropDuplicates(
        ["full_date"]
    )
    .withColumn(
        "date_key",
        F.date_format(
            F.col("full_date"),
            "yyyyMMdd",
        ).cast("integer"),
    )
    .withColumn(
        "day_of_month",
        F.dayofmonth("full_date"),
    )
    .withColumn(
        "day_of_week",
        F.dayofweek("full_date"),
    )
    .withColumn(
        "day_name",
        F.date_format(
            F.col("full_date"),
            "EEEE",
        ),
    )
    .withColumn(
        "week_of_year",
        F.weekofyear("full_date"),
    )
    .withColumn(
        "month_number",
        F.month("full_date"),
    )
    .withColumn(
        "month_name",
        F.date_format(
            F.col("full_date"),
            "MMMM",
        ),
    )
    .withColumn(
        "quarter_number",
        F.quarter("full_date"),
    )
    .withColumn(
        "year_number",
        F.year("full_date"),
    )
    .withColumn(
        "year_month",
        F.date_format(
            F.col("full_date"),
            "yyyy-MM",
        ),
    )
    .withColumn(
        "is_weekend",
        F.dayofweek(
            F.col("full_date")
        ).isin(1, 7),
    )
    .withColumn(
        "_gold_processed_at",
        F.current_timestamp(),
    )
    .select(
        "date_key",
        "full_date",
        "day_of_month",
        "day_of_week",
        "day_name",
        "week_of_year",
        "month_number",
        "month_name",
        "quarter_number",
        "year_number",
        "year_month",
        "is_weekend",
        "_gold_processed_at",
    )
)

display(
    dim_date_df.orderBy("full_date").limit(10)
)

# COMMAND ----------

write_gold_table(
    dataframe=dim_customers_df,
    table_name="dim_customers",
)

write_gold_table(
    dataframe=dim_products_df,
    table_name="dim_products",
)

write_gold_table(
    dataframe=dim_sellers_df,
    table_name="dim_sellers",
)

write_gold_table(
    dataframe=dim_date_df,
    table_name="dim_date",
)

# COMMAND ----------

order_context_df = (
    orders_df
    .select(
        "order_id",
        "customer_id",
        "purchase_date",
        "order_status",
        "order_purchase_timestamp",
        "delivery_days",
        "is_delivered_late",
    )
)

# COMMAND ----------

fct_order_items_df = (
    order_items_df.alias("items")

    .join(
        order_context_df.alias("orders"),
        on="order_id",
        how="inner",
    )

    .join(
        dim_customers_df
        .select(
            "customer_id",
            "customer_key",
        )
        .alias("customers"),
        on="customer_id",
        how="inner",
    )

    .join(
        dim_products_df
        .select(
            "product_id",
            "product_key",
        )
        .alias("products"),
        on="product_id",
        how="inner",
    )

    .join(
        dim_sellers_df
        .select(
            "seller_id",
            "seller_key",
        )
        .alias("sellers"),
        on="seller_id",
        how="inner",
    )

    .select(
        F.sha2(
            F.concat_ws(
                "||",
                F.col("order_id"),
                F.col(
                    "order_item_id"
                ).cast("string"),
            ),
            256,
        ).alias("order_item_key"),

        F.col("order_id"),
        F.col("order_item_id"),

        F.col("customer_key"),
        F.col("product_key"),
        F.col("seller_key"),

        F.date_format(
            F.col("purchase_date"),
            "yyyyMMdd",
        ).cast("integer").alias(
            "order_date_key"
        ),

        F.col("order_status"),
        F.col("shipping_limit_date"),

        F.col("price").alias(
            "item_price"
        ),

        F.col("freight_value"),

        F.col("item_total_amount"),

        F.col("delivery_days"),
        F.col("is_delivered_late"),

        F.lit(1).alias("item_quantity"),

        F.current_timestamp().alias(
            "_gold_processed_at"
        ),
    )
)

display(fct_order_items_df.limit(10))

# COMMAND ----------

write_gold_table(
    dataframe=fct_order_items_df,
    table_name="fct_order_items",
)

# COMMAND ----------

order_items_agg_df = (
    order_items_df
    .groupBy("order_id")
    .agg(
        F.count("*").alias(
            "item_count"
        ),

        F.countDistinct(
            "product_id"
        ).alias(
            "distinct_product_count"
        ),

        F.countDistinct(
            "seller_id"
        ).alias(
            "distinct_seller_count"
        ),

        F.sum("price").alias(
            "item_subtotal"
        ),

        F.sum("freight_value").alias(
            "freight_total"
        ),

        F.sum(
            "item_total_amount"
        ).alias(
            "order_total"
        ),
    )
)

# COMMAND ----------

payments_agg_df = (
    payments_df
    .groupBy("order_id")
    .agg(
        F.count("*").alias(
            "payment_record_count"
        ),

        F.countDistinct(
            "payment_type"
        ).alias(
            "payment_type_count"
        ),

        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set(
                    "payment_type"
                )
            ),
        ).alias(
            "payment_types"
        ),

        F.max(
            "payment_installments"
        ).alias(
            "maximum_installments"
        ),

        F.sum(
            "payment_value"
        ).alias(
            "payment_total"
        ),
    )
)


# COMMAND ----------

reviews_agg_df = (
    reviews_df
    .groupBy("order_id")
    .agg(
        F.countDistinct(
            "review_id"
        ).alias(
            "review_count"
        ),

        F.round(
            F.avg("review_score"),
            2,
        ).alias(
            "average_review_score"
        ),

        F.max(
            F.col(
                "has_comment"
            ).cast("integer")
        ).cast("boolean").alias(
            "has_review_comment"
        ),
    )
)

# COMMAND ----------

zero_decimal = F.lit(0).cast(
    "decimal(18,2)"
)


fct_orders_df = (
    orders_df.alias("orders")

    .join(
        dim_customers_df
        .select(
            "customer_id",
            "customer_key",
        )
        .alias("customers"),
        on="customer_id",
        how="inner",
    )

    .join(
        order_items_agg_df.alias("items"),
        on="order_id",
        how="left",
    )

    .join(
        payments_agg_df.alias("payments"),
        on="order_id",
        how="left",
    )

    .join(
        reviews_agg_df.alias("reviews"),
        on="order_id",
        how="left",
    )

    .select(
        F.sha2(
            F.col("order_id"),
            256,
        ).alias("order_key"),

        F.col("order_id"),
        F.col("customer_key"),

        F.date_format(
            F.col("purchase_date"),
            "yyyyMMdd",
        ).cast("integer").alias(
            "order_date_key"
        ),

        F.col("order_status"),
        F.col("order_purchase_timestamp"),
        F.col("order_approved_at"),
        F.col("order_delivered_carrier_date"),
        F.col("order_delivered_customer_date"),
        F.col("order_estimated_delivery_date"),

        F.col("delivery_days"),
        F.col("is_delivered_late"),

        F.coalesce(
            F.col("item_count"),
            F.lit(0),
        ).alias("item_count"),

        F.coalesce(
            F.col("distinct_product_count"),
            F.lit(0),
        ).alias(
            "distinct_product_count"
        ),

        F.coalesce(
            F.col("distinct_seller_count"),
            F.lit(0),
        ).alias(
            "distinct_seller_count"
        ),

        F.coalesce(
            F.col("item_subtotal"),
            zero_decimal,
        ).alias("item_subtotal"),

        F.coalesce(
            F.col("freight_total"),
            zero_decimal,
        ).alias("freight_total"),

        F.coalesce(
            F.col("order_total"),
            zero_decimal,
        ).alias("order_total"),

        F.coalesce(
            F.col("payment_record_count"),
            F.lit(0),
        ).alias(
            "payment_record_count"
        ),

        F.coalesce(
            F.col("payment_type_count"),
            F.lit(0),
        ).alias(
            "payment_type_count"
        ),

        F.col("payment_types"),

        F.coalesce(
            F.col("maximum_installments"),
            F.lit(0),
        ).alias(
            "maximum_installments"
        ),

        F.coalesce(
            F.col("payment_total"),
            zero_decimal,
        ).alias("payment_total"),

        F.col("review_count"),
        F.col("average_review_score"),
        F.col("has_review_comment"),

        F.lit(1).alias("order_count"),

        F.current_timestamp().alias(
            "_gold_processed_at"
        ),
    )

    .withColumn(
        "payment_difference",
        F.round(
            F.col("payment_total")
            - F.col("order_total"),
            2,
        ),
    )
)

display(fct_orders_df.limit(10))

# COMMAND ----------

write_gold_table(
    dataframe=fct_orders_df,
    table_name="fct_orders",
)

# COMMAND ----------

silver_order_count = orders_df.count()
gold_order_count = fct_orders_df.count()

silver_item_count = order_items_df.count()
gold_item_count = fct_order_items_df.count()


assert gold_order_count == silver_order_count, (
    "fct_orders count does not match Silver orders."
)

assert gold_item_count == silver_item_count, (
    "fct_order_items count does not match "
    "Silver order_items."
)


print(
    f"✅ fct_orders: "
    f"{gold_order_count:,} rows"
)

print(
    f"✅ fct_order_items: "
    f"{gold_item_count:,} rows"
)

# COMMAND ----------

null_order_keys = (
    fct_orders_df
    .filter(
        F.col("order_key").isNull()
        |
        F.col("customer_key").isNull()
        |
        F.col("order_date_key").isNull()
    )
    .count()
)


null_item_keys = (
    fct_order_items_df
    .filter(
        F.col("order_item_key").isNull()
        |
        F.col("customer_key").isNull()
        |
        F.col("product_key").isNull()
        |
        F.col("seller_key").isNull()
        |
        F.col("order_date_key").isNull()
    )
    .count()
)


print(
    f"Null fact order keys: "
    f"{null_order_keys}"
)

print(
    f"Null fact item keys: "
    f"{null_item_keys}"
)


assert null_order_keys == 0
assert null_item_keys == 0

print("✅ Gold key validation passed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     'dim_customers' AS table_name,
# MAGIC     COUNT(*) AS row_count
# MAGIC FROM workspace.ecommerce_gold.dim_customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'dim_products',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_gold.dim_products
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'dim_sellers',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_gold.dim_sellers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'dim_date',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_gold.dim_date
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'fct_orders',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_gold.fct_orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'fct_order_items',
# MAGIC     COUNT(*)
# MAGIC FROM workspace.ecommerce_gold.fct_order_items
# MAGIC
# MAGIC ORDER BY table_name;