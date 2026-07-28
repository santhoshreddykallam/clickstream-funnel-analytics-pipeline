import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\hp\AppData\Local\Programs\Python\Python310\python.exe"

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ClickstreamIngestion').getOrCreate()

df_pyspark = spark.read.csv('2019-Oct.csv', header = True, inferSchema = True)

df_pyspark.printSchema()
df_pyspark.count()
df_pyspark.show(5)