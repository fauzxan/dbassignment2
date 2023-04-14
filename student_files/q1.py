import sys
from pyspark.sql import SparkSession
# you may add more import if you need to


# don't change this line
hdfs_nn = "172.31.88.194"

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header",True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/")
df.printSchema()

df_filtered = df.filter(df.Rating > 1.0)

# Print the filtered rows
df_filtered.show()