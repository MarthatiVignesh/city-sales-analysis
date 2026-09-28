from pyspark.sql import SparkSession

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("CitySalesAnalysis") \
    .master("local[*]") \
    .getOrCreate()

print("Spark Session Started Successfully")


# --------------------------------------------------
# 2. Read Customers Data
# --------------------------------------------------
customers_df = spark.read.csv(
    "file:///home/sri/city-sales-analysis/data/customers.csv",
    header=True,
    inferSchema=True
)


# --------------------------------------------------
# 3. Read Orders Data
# --------------------------------------------------
orders_df = spark.read.csv(
    "file:///home/sri/city-sales-analysis/data/orders.csv",
    header=True,
    inferSchema=True
)


# --------------------------------------------------
# 4. Read Products Data
# --------------------------------------------------
products_df = spark.read.csv(
    "file:///home/sri/city-sales-analysis/data/products.csv",
    header=True,
    inferSchema=True
)


# --------------------------------------------------
# 5. Cleaning Step - Remove Rows with Missing Keys
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
# 10. Stop Spark
# --------------------------------------------------
spark.stop()
