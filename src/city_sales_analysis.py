from pyspark.sql import SparkSession
import logging

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------
logging.getLogger("py4j").setLevel(logging.ERROR)

spark = SparkSession.builder \
    .appName("CitySalesAnalysis") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Spark Session Started Successfully")


# --------------------------------------------------
# 2. Read Customers Data
# --------------------------------------------------
customers_df = spark.read.csv(
    "data/customers.csv",
    header=True,
    inferSchema=True
)


# --------------------------------------------------
# 3. Read Orders Data
# --------------------------------------------------
orders_df = spark.read.csv(
    "data/orders.csv",
    header=True,
    inferSchema=True
)


# --------------------------------------------------
# 4. Read Products Data
# --------------------------------------------------
products_df = spark.read.csv(
    "data/products.csv",
    header=True,
    inferSchema=True
)


# --------------------------------------------------
# VIGNESH'S PART
# 5. Cleaning Step
# --------------------------------------------------
print("\nOrders count before cleaning:", orders_df.count())

orders_df = orders_df.dropna(
    subset=["order_id", "customer_id", "product_id"]
)

print("Orders count after cleaning:", orders_df.count())


# --------------------------------------------------
# 6. Filter Completed Orders
# --------------------------------------------------
completed_orders_df = orders_df.filter(
    orders_df.status == "complete"
)

print("\nCompleted Orders Count:", completed_orders_df.count())


# --------------------------------------------------
# 7. Display Schemas
# --------------------------------------------------
print("\nCustomers Schema:")
customers_df.printSchema()

print("\nOrders Schema:")
orders_df.printSchema()

print("\nProducts Schema:")
products_df.printSchema()


# --------------------------------------------------
# 8. Show Sample Rows
# --------------------------------------------------
print("\nCustomers Sample:")
customers_df.show(5, truncate=False)

print("\nOrders Sample:")
orders_df.show(5, truncate=False)

print("\nProducts Sample:")
products_df.show(5, truncate=False)


# --------------------------------------------------
# 9. Show Record Counts
# --------------------------------------------------
print("\nRecord Counts:")

print("Customers:", customers_df.count())
print("Orders:", orders_df.count())
print("Products:", products_df.count())
print("Completed Orders:", completed_orders_df.count())


# --------------------------------------------------

# 10. Data Quality Checks
# --------------------------------------------------
print("\n==========DATA QUALITY CHECKS ==========")


# Missing IDs
missing_order_ids = orders_df.filter(
    orders_df.order_id.isNull()
).count()

missing_customer_ids = orders_df.filter(
    orders_df.customer_id.isNull()
).count()

missing_product_ids = orders_df.filter(
    orders_df.product_id.isNull()
).count()

print("Missing order IDs:", missing_order_ids)
print("Missing customer IDs:", missing_customer_ids)
print("Missing product IDs:", missing_product_ids)


# Duplicate Order IDs
duplicate_order_ids = orders_df.groupBy("order_id") \
    .count() \
    .filter("count > 1") \
    .count()

print("Duplicate order IDs:", duplicate_order_ids)


# Invalid Customer IDs
invalid_customer_orders = orders_df.join(
    customers_df,
    on="customer_id",
    how="left_anti"
)

print(
    "Orders with invalid customer IDs:",
    invalid_customer_orders.count()
)

print("\nInvalid Customer Orders:")
invalid_customer_orders.show(truncate=False)


# Invalid Product IDs
invalid_product_orders = orders_df.join(
    products_df,
    on="product_id",
    how="left_anti"
)

print(
    "Orders with invalid product IDs:",
    invalid_product_orders.count()
)

print("\nInvalid Product Orders:")
invalid_product_orders.show(truncate=False)


# --------------------------------------------------
# 11. Explain Plan
# --------------------------------------------------
print("\n========== EXPLAIN PLAN ==========")

completed_orders_df.explain()


# --------------------------------------------------
# 12. Stop Spark
# --------------------------------------------------
spark.stop()
