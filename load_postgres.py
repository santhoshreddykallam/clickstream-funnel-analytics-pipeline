import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\hp\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PG_PASSWORD"] = "Santhu@123"

import sys
import psycopg2
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    when,
    lit,
    lag,
    round
)
from pyspark.sql.window import Window
from datetime import datetime

# -----------------------------
# Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("Load Funnel Summary to PostgreSQL")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

# -----------------------------
# Input File
# -----------------------------
input_file = sys.argv[1]

# Extract month from filename
month = os.path.splitext(os.path.basename(input_file))[0]

# Convert to a Python date (e.g., 2019-10-01)
month_start_date = datetime.strptime(month, "%Y-%b").date()

# -----------------------------
# Read CSV
# -----------------------------
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_file)
)

# -----------------------------
# Cleaning (same as transform.py)
# -----------------------------
df_clean = (
    df.filter(
        col("event_type").isin(
            "view",
            "cart",
            "purchase"
        )
    )
)

# -----------------------------
# Stage Mapping
# -----------------------------
df_stage = (
    df_clean.withColumn(
        "funnel_stage",
        when(col("event_type") == "view", 1)
        .when(col("event_type") == "cart", 2)
        .when(col("event_type") == "purchase", 3)
    )
)

# -----------------------------
# Funnel Summary
# -----------------------------
funnel_summary = (
    df_stage
    .groupBy("funnel_stage")
    .agg(
        countDistinct("user_session").alias("unique_sessions")
    )
    .orderBy("funnel_stage")
)

funnel_summary = funnel_summary.filter(col("funnel_stage").isNotNull())

cart_removal_sessions = df.filter(col("event_type") == "remove_from_cart").agg(
    countDistinct("user_session").alias("cart_removal_sessions"))

cart_removal_count = cart_removal_sessions.collect()[0][0]

# -----------------------------
# Add stage_name
# -----------------------------
funnel_summary = (
    funnel_summary.withColumn(
        "stage_name",
        when(col("funnel_stage") == 1, "view")
        .when(col("funnel_stage") == 2, "cart")
        .when(col("funnel_stage") == 3, "purchase")
    )
)

# -----------------------------
# Calculate previous_stage_sessions
# and drop_off_rate
# -----------------------------
window_spec = Window.orderBy("funnel_stage")

funnel_summary = (
    funnel_summary
    .withColumn(
        "previous_stage_sessions",
        lag("unique_sessions").over(window_spec)
    )
    .withColumn(
        "drop_off_rate",
        when(
            col("previous_stage_sessions").isNull(),
            0.0
        ).otherwise(
            round(
                (
                    (col("previous_stage_sessions") - col("unique_sessions"))
                    / col("previous_stage_sessions")
                ) * 100,
                2
            )
        )
    )
)

# -----------------------------
# Add month
# -----------------------------
funnel_summary = (
    funnel_summary
    .withColumn("month", lit(month))
    .withColumn("month_start_date", lit(month_start_date))
)

# -----------------------------
# Convert to Pandas
# -----------------------------
pdf = funnel_summary.toPandas()

# -----------------------------
# Load into PostgreSQL
# -----------------------------
conn = psycopg2.connect(
    host="host.docker.internal",
    database="clickstream_db",
    user="postgres",
    password=os.environ.get("PG_PASSWORD")
)

cursor = conn.cursor()

insert_query = """
INSERT INTO funnel_summary
(
    funnel_stage,
    stage_name,
    unique_sessions,
    previous_stage_sessions,
    drop_off_rate,
    cart_removal_sessions,
    month,
    month_start_date
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

for _, row in pdf.iterrows():
    cursor.execute(
        insert_query,
        (
            int(row["funnel_stage"]),
            row["stage_name"],
            int(row["unique_sessions"]),
            None if pd.isna(row["previous_stage_sessions"]) else int(row["previous_stage_sessions"]),
            float(row["drop_off_rate"]),
            int(cart_removal_count),
            row["month"],
            row["month_start_date"]
        )
    )

conn.commit()

cursor.close()
conn.close()

spark.stop()

print(f"Successfully loaded funnel summary for {month} into PostgreSQL.")