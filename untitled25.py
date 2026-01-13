from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ecommerce Big Data Analysis") \
    .getOrCreate()
data = [
    ("Lahore", "Electronics", 50000),
    ("Karachi", "Clothing", 20000),
    ("Lahore", "Clothing", 15000),
    ("Islamabad", "Electronics", 60000),
    ("Karachi", "Electronics", 45000),
    ("Lahore", "Grocery", 8000),
    ("Islamabad", "Grocery", 12000),
]

columns = ["city", "category", "order_amount"]

df = spark.createDataFrame(data, columns)
df.show()
from pyspark.sql.functions import sum

revenue_by_city = df.groupBy("city") \
    .agg(sum("order_amount").alias("total_revenue"))

revenue_by_city.show()
revenue_by_category = df.groupBy("category") \
    .agg(sum("order_amount").alias("category_revenue"))

revenue_by_category.show()
from pyspark.sql.functions import avg

avg_order = df.agg(avg("order_amount").alias("avg_order_value"))
avg_order.show()
