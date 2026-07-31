import os
import sys

os.environ["PYSPARK_PYTHON"] = r"C:\Users\hp\AppData\Local\Programs\Python\Python310\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, countDistinct, lag, round
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('ClickstreamTransform').getOrCreate()

filename = sys.argv[1]

df_pyspark = spark.read.csv(filename, header = True, inferSchema = True)

df_pyspark = df_pyspark.drop('category_code')
df_pyspark = df_pyspark.na.drop(how = 'any', subset = ['user_session'])

df_pyspark = df_pyspark.withColumn(
    "funnel_stage",
    when(col("event_type") == "view", 1)
    .when(col("event_type") == "cart", 2)
    .when(col("event_type") == "purchase", 3)
)

df_pyspark.select(
    "event_type",
    "funnel_stage",
    "user_session"
).show(10, truncate = False)

df_pyspark.write.mode("overwrite").parquet("output/parquet/")

funnel_summary = df_pyspark.groupby("funnel_stage").agg(
    countDistinct("user_session").alias("unique_sessions")
    ).orderBy("funnel_stage")

funnel_summary = funnel_summary.filter(col("funnel_stage").isNotNull())

cart_removal_sessions = df_pyspark.filter(col("event_type") == "remove_from_cart").agg(
    countDistinct("user_session").alias("cart_removal_sessions"))

window_spec = Window.orderBy("funnel_stage")

funnel_summary = funnel_summary.withColumn(
    "previous_stage_sessions",
    lag("unique_sessions", 1).over(window_spec)
)

funnel_summary = funnel_summary.withColumn(
    "drop_off_rate",
    when(
        col("previous_stage_sessions").isNull(),
        0
    ).otherwise(
        round(
            (1 - (col("unique_sessions") / col("previous_stage_sessions"))) * 100,
            2
        )
    )
)

funnel_summary.show()
cart_removal_sessions.show()

df_pyspark.printSchema()

print("Total rows after cleaning:", df_pyspark.count())

spark.stop()