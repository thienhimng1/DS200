import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    sum,
    round
)

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("Seller Ranking Analysis") \
    .getOrCreate()

# Đọc dữ liệu
order_items_df = spark.read.csv(
    "Order_Items.csv",
    header=True,
    inferSchema=True,
    sep=";"
)

# 10. Xếp hạng các seller dựa trên tổng doanh thu và số lượng đơn hàng bán được.
seller_ranking = order_items_df \
    .withColumn(
        "Revenue",
        col("Price") + col("Freight_Value")
    ) \
    .groupBy("Seller_ID") \
    .agg(
        round(sum("Revenue"), 2).alias("Total_Revenue"),
        countDistinct("Order_ID").alias("Total_Orders")
    ) \
    .orderBy(
        col("Total_Revenue").desc(),
        col("Total_Orders").desc()
    )

print("\n===== SELLER RANKING =====")

seller_ranking.show(truncate=False)