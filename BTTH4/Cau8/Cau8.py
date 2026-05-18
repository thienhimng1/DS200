import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    datediff,
    when,
    to_timestamp
)

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("E-commerce Data Analysis") \
    .getOrCreate()

# Đọc dữ liệu
orders_df = spark.read.csv(
    "Orders.csv",
    header=True,
    inferSchema=True,
    sep=";"
)

order_items_df = spark.read.csv(
    "Order_Items.csv",
    header=True,
    inferSchema=True,
    sep=";"
)

# 8.  Tính toán hiệu số giữa ngày giao hàng thực tế (Order_Delivered_Carrier_Date) và ngày giao hàng
# dự kiến (ví dụ: Shipping_Limit_Date từ bảng Order_Items) để đánh giá hiệu suất giao hàng.
delivery_performance = orders_df \
    .filter(col("Order_Delivered_Carrier_Date").isNotNull()) \
    .join(order_items_df, "Order_ID", "inner") \
    .withColumn(
        "Actual_Date",
        to_timestamp(
            "Order_Delivered_Carrier_Date",
            "yyyy-MM-dd HH:mm"
        )
    ) \
    .withColumn(
        "Expected_Date",
        to_timestamp(
            "Shipping_Limit_Date",
            "yyyy-MM-dd HH:mm"
        )
    ) \
    .withColumn(
        "Delivery_Diff_Days",
        datediff(
            col("Actual_Date"),
            col("Expected_Date")
        )
    ) \
    .withColumn(
        "Delivery_Status",
        when(col("Delivery_Diff_Days") > 0, "Late")
        .otherwise("On Time / Early")
    ) \
    .select(
        "Order_ID",
        "Product_ID",
        "Actual_Date",
        "Expected_Date",
        "Delivery_Diff_Days",
        "Delivery_Status"
    )

print("\n===== DELIVERY PERFORMANCE ANALYSIS =====")

delivery_performance.show(truncate=False)