import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    expr,
    round
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

# Làm sạch Review_Score
cleaned_reviews_df = order_reviews_df \
    .withColumn(
        "Review_Score_Int",
        expr("try_cast(Review_Score as int)")
    ) \
    .filter(
        col("Review_Score_Int").between(1, 5)
    )

# 7.  Xác định sản phẩm có số lượng bán ra cao nhất và tính điểm đánh giá trung bình cho từng sản phẩm
product_stats = order_items_df \
    .join(products_df, "Product_ID", "inner") \
    .join(cleaned_reviews_df, "Order_ID", "left") \
    .groupBy("Product_Category_Name") \
    .agg(
        count("Order_Item_ID").alias("Total_Orders"),
        round(
            avg("Review_Score_Int"),
            2
        ).alias("Average_Review_Score")
    ) \
    .orderBy(col("Total_Orders").desc())

print("\n===== PRODUCT CATEGORY ANALYSIS =====")

product_stats.show(truncate=False)