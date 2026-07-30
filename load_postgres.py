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

# -----------------------------
# Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("Load Funnel Summary to PostgreSQL")
    .getOrCreate()
)

# -----------------------------
# Input File
# -----------------------------
input_file = sys.argv[1]

# Extract month from filename
month = os.path.splitext(os.path.basename(input_file))[0]

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
            "remove_from_cart",
            "purchase"
        )
    )
)

# -----------------------------
# Stage Mapping
# -----------------------------
df_stage = (
    df_clean.withColumn(
        "stage",
        when(col("event_type") == "view", 1)
        .when(col("event_type") == "cart", 2)
        .when(col("event_type") == "remove_from_cart", 3)
        .when(col("event_type") == "purchase", 4)
    )
)

# -----------------------------
# Funnel Summary
# -----------------------------
funnel_summary = (
    df_stage
    .groupBy("stage")
    .agg(
        countDistinct("user_session").alias("unique_sessions")
    )
    .orderBy("stage")
)

# -----------------------------
# Rename stage -> funnel_stage
# -----------------------------
funnel_summary = funnel_summary.withColumnRenamed(
    "stage",
    "funnel_stage"
)

# -----------------------------
# Add stage_name
# -----------------------------
funnel_summary = (
    funnel_summary.withColumn(
        "stage_name",
        when(col("funnel_stage") == 1, "view")
        .when(col("funnel_stage") == 2, "cart")
        .when(col("funnel_stage") == 3, "remove_from_cart")
        .when(col("funnel_stage") == 4, "purchase")
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
funnel_summary = funnel_summary.withColumn(
    "month",
    lit(month)
)

# -----------------------------
# Convert to Pandas
# -----------------------------
pdf = funnel_summary.toPandas()

# -----------------------------
# Load into PostgreSQL
# -----------------------------
conn = psycopg2.connect(
    host="localhost",
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
    month
)
VALUES (%s, %s, %s, %s, %s, %s)
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
            row["month"]
        )
    )

conn.commit()

cursor.close()
conn.close()

spark.stop()

print(f"Successfully loaded funnel summary for {month} into PostgreSQL.")