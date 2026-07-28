import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\hp\AppData\Local\Programs\Python\Python310\python.exe"

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ClickstreamTransform').getOrCreate()

df_pyspark = spark.read.csv('2019-Oct.csv', header = True, inferSchema = True)

df_pyspark = df_pyspark.drop('category_code')
df_pyspark = df_pyspark.na.drop(how = 'any', subset = ['user_session'])

df_pyspark.printSchema()

print("Total rows after cleaning:", df_pyspark.count())

spark.stop()