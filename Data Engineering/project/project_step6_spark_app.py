import os
from pyspark.sql import SparkSession

os.environ["SPARK_HOME"] = "/usr/local/spark"
os.environ["PYSPARK_PYTHON"] = "/home/pigidser/anaconda3/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
os.environ["PYSPARK_SUBMIT_ARGS"] = "pyspark-shell"

master = "local"
spark = SparkSession.builder.master(master).appName("spark_app").getOrCreate()

# Load data from csv-file
df_movies = spark.read.format("csv") \
    .option("mode", "FAILFAST") \
    .option("inferSchema", "true") \
    .option("header","true") \
    .option("path", "movies.csv") \
    .load()

df_movies.write.format("csv") \
    .mode("overwrite") \
    .option("sep", "\t") \
    .save("new_movies_spark_app.csv")

spark.stop()