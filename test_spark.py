import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\hp\AppData\Local\Programs\Python\Python310\python.exe"

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("TestSetup").getOrCreate()
print("Spark version:", spark.version)

df = spark.createDataFrame([(1, "hello"), (2, "world")], ["id", "message"])
df.show()