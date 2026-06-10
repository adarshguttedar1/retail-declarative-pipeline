# =============================================================
# GOLD LAYER - PySpark
# Medallion Architecture | Databricks Asset Bundles
# =============================================================
# Silver Tables:
#   az_adb_simbus_training.retail.silver_orders
#   az_adb_simbus_training.retail.silver_order_items
#   az_adb_simbus_training.retail.silver_products
#   az_adb_simbus_training.retail.silver_customers

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

CATALOG = "az_adb_simbus_training"
SCHEMA  = "retail"

# Ensure gold schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# ── Load silver tables ────────────────────────────────────────
orders      = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_orders")
order_items = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_order_items")
products    = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_products")
customers   = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_customers")

# Shared filter — exclude canceled / unavailable orders
EXCLUDED_STATUSES = ["canceled", "unavailable"]


# =============================================================
# 1. gold_sales_summary
#    Metrics: Total Revenue, Daily Sales Trend, Monthly Sales Trend
# =============================================================

valid_orders = orders.filter(
    ~F.col("order_status").isin(EXCLUDED_STATUSES) &
    F.col("order_purchase_timestamp").isNotNull()
)

sales_joined = valid_orders.join(order_items, on="order_id", how="inner")

gold_sales_summary = (
    sales_joined
    .withColumn("order_date",  F.to_date("order_purchase_timestamp"))
    .withColumn("order_month", F.date_format("order_purchase_timestamp", "yyyy-MM"))
    .withColumn("order_year",  F.year("order_purchase_timestamp"))
    .groupBy("order_date", "order_month", "order_year")
    .agg(
        F.round(F.sum("price"),                                          2).alias("total_revenue"),
        F.round(F.sum("freight_value"),                                  2).alias("total_freight"),
        F.round(F.sum(F.col("price") + F.col("freight_value")),          2).alias("gross_revenue"),
        F.countDistinct("order_id")                                       .alias("total_orders"),
        F.count("order_item_id")                                          .alias("total_items_sold"),
    )
    .withColumn(
        "avg_order_value",
        F.round(F.col("total_revenue") / F.nullif(F.col("total_orders"), F.lit(0)), 2)
    )
    .orderBy("order_date")
)

(
    gold_sales_summary
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_sales_summary")
)
print(" gold_sales_summary written successfully")


# =============================================================
# 2. gold_product_performance
#    Metrics: Top Selling Products, Revenue by Product & Category
# =============================================================

product_joined = (
    order_items
    .join(products, on="product_id", how="inner")
    .join(
        valid_orders.select("order_id"),
        on="order_id",
        how="inner"
    )
)

product_agg = (
    product_joined
    .groupBy("product_id", "product_category_name")
    .agg(
        F.count("order_item_id")                                          .alias("total_units_sold"),
        F.countDistinct("order_id")                                       .alias("total_orders"),
        F.round(F.sum("price"),                                       2)  .alias("total_revenue"),
        F.round(F.sum("freight_value"),                               2)  .alias("total_freight_revenue"),
        F.round(F.sum(F.col("price") + F.col("freight_value")),       2)  .alias("gross_revenue"),
        F.round(F.avg("price"),                                       2)  .alias("avg_unit_price"),
    )
)

# Window specs for ranking
window_category = Window.partitionBy("product_category_name").orderBy(F.col("total_revenue").desc())
window_overall  = Window.orderBy(F.col("total_revenue").desc())

gold_product_performance = (
    product_agg
    .withColumn("revenue_rank_in_category", F.rank().over(window_category))
    .withColumn("overall_revenue_rank",     F.rank().over(window_overall))
    .orderBy(F.col("total_revenue").desc())
)

(
    gold_product_performance
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_product_performance")
)
print(" gold_product_performance written successfully")


# =============================================================
# 3. gold_repeat_customers
#    Metrics: Repeat Customers, Average Order Value per Customer
# =============================================================

# CTE equivalent: order-level aggregation per unique customer
customer_orders = (
    customers
    .join(valid_orders, on="customer_id", how="inner")
    .join(order_items,  on="order_id",    how="inner")
    .groupBy(
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "order_id",
        "order_purchase_timestamp"
    )
    .agg(
        F.round(F.sum("price"),                                      2).alias("order_revenue"),
        F.round(F.sum("freight_value"),                              2).alias("order_freight"),
        F.round(F.sum(F.col("price") + F.col("freight_value")),      2).alias("order_gross_value"),
    )
)

# Customer-level aggregation
gold_repeat_customers = (
    customer_orders
    .groupBy("customer_unique_id", "customer_city", "customer_state")
    .agg(
        F.countDistinct("order_id")                                       .alias("total_orders"),
        F.round(F.sum("order_revenue"),                              2)   .alias("total_revenue"),
        F.round(F.sum("order_gross_value"),                          2)   .alias("total_gross_value"),
        F.round(F.avg("order_revenue"),                              2)   .alias("avg_order_value"),
        F.round(F.avg("order_gross_value"),                          2)   .alias("avg_gross_order_value"),
        F.to_date(F.min("order_purchase_timestamp"))                       .alias("first_order_date"),
        F.to_date(F.max("order_purchase_timestamp"))                       .alias("last_order_date"),
    )
    .withColumn(
        "is_repeat_customer",
        F.when(F.col("total_orders") > 1, True).otherwise(False)
    )
    .withColumn(
        "customer_segment",
        F.when(F.col("total_orders") == 1,  "One-Time")
         .when(F.col("total_orders") <= 3,  "Occasional")
         .otherwise("Loyal")
    )
    .withColumn(
        "customer_lifespan_days",
        F.datediff(F.col("last_order_date"), F.col("first_order_date"))
    )
    .orderBy(
        F.col("total_orders").desc(),
        F.col("total_revenue").desc()
    )
)

(
    gold_repeat_customers
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_repeat_customers")
)
print(" gold_repeat_customers written successfully")