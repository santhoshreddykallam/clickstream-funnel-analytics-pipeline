import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\hp\AppData\Local\Programs\Python\Python310\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('DataQualityCheck').getOrCreate()

df_pyspark = spark.read.csv('2019-Oct.csv', header=True, inferSchema=True)

total_rows = df_pyspark.count()

print(f"Total Rows: {total_rows}\n")

print(f"{'Column':<20}{'Null Count':<15}{'Null Percentage'}")
print("-" * 50)

for column in df_pyspark.columns:
    null_count = df_pyspark.filter(col(column).isNull()).count()
    null_percentage = (null_count / total_rows) * 100

    print(f"{column:<20}{null_count:<15}{null_percentage:.2f}%")

spark.stop()