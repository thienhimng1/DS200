import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    avg,
    sum,
    round,
    when
)

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("E-commerce Data Analysis") \
    .getOrCreate()

# Đọc dữ liệu từ các file csv và tự suy luận kiểu dữ liệu
customers_df = spark.read.csv("Customer_List.csv", header=True, inferSchema=True, sep=";")
order_items_df = spark.read.csv("Order_Items.csv", header=True, inferSchema=True, sep=";")
order_reviews_df = spark.read.csv("Order_Reviews.csv", header=True, inferSchema=True, sep=";")
orders_df = spark.read.csv("Orders.csv", header=True, inferSchema=True, sep=";")
products_df = spark.read.csv("Products.csv", header=True, inferSchema=True, sep=";")

# 9.  Nhóm khách hàng dựa trên số lượng đơn hàng, giá trị trung bình của đơn hàng và tần suất mua sắm.
customer_orders = orders_df \
    .join(order_items_df, "Order_ID", "inner") \
    .withColumn(
        "Order_Value",
        col("Price") + col("Freight_Value")
    )

customer_stats = customer_orders \
    .groupBy("Customer_Trx_ID") \
    .agg(
        countDistinct("Order_ID").alias("Total_Orders"),
        round(avg("Order_Value"), 2).alias("Avg_Order_Value")
    )

customer_segments = customer_stats.withColumn(
    "Customer_Group",
    when(
        (col("Total_Orders") >= 10) &
        (col("Avg_Order_Value") >= 500),
        "VIP Customer"
    ).when(
        (col("Total_Orders") >= 5),
        "Frequent Customer"
    ).otherwise(
        "Regular Customer"
    )
)

print("\n===== CUSTOMER SEGMENTATION =====")

customer_segments.show(truncate=False)