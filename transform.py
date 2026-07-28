import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\hp\AppData\Local\Programs\Python\Python310\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
spark = SparkSession.builder.appName('ClickstreamTransform').getOrCreate()

df_pyspark = spark.read.csv('2019-Oct.csv', header = True, inferSchema = True)

df_pyspark = df_pyspark.drop('category_code')
df_pyspark = df_pyspark.na.drop(how = 'any', subset = ['user_session'])

df_pyspark = df_pyspark.withColumn(
    "funnel_stage",
    when(col("event_type") == "view", 1)
    .when(col("event_type") == "cart", 2)
    .when(col("event_type") == "remove_from_cart", 3)
    .otherwise(4)
)

df_pyspark.select(
    "event_type",
    "funnel_stage",
    "user_session"
).show(10, truncate = False)

df_pyspark.printSchema()

print("Total rows after cleaning:", df_pyspark.count())

spark.stop()