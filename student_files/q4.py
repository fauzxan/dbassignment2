import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, regexp_replace, split, trim, col,count, desc




# don't change this line
try:
    hdfs_nn = sys.argv[1].strip()
    print("\n\n\n\nSuccessfully retrieved system argument: ", hdfs_nn)
except Exception as e: 
    print(f"Error: {e}")
    print("<Usage>: spark-submit spark://<master>:7077 q1.py <hdfs_namenode>")
    sys.exit(1)
try:
    spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
except Exception as e:
    print(f"\n\nError while trying to create session: {e}")
    sys.exit(1)
# YOUR CODE GOES BELOW

#AWS Academy
df = spark.read.option("header", True).option("inferSchema", True).csv("hdfs://{}:9000/assignment2/part1/input/".format(hdfs_nn))


df = df.withColumn("Cuisine Style", explode(split(col("Cuisine Style"), ", "))).select("City","Cuisine Style")
df = df.withColumn("Cuisine Style", explode(split(col("Cuisine Style"), ", "))).select("City","Cuisine Style")
#formatting
df = df.withColumn("Cuisine Style", regexp_replace("Cuisine Style", "\\[", "")).withColumn("Cuisine Style", regexp_replace("Cuisine Style", "\\]", ""))
changed_df= df.withColumn("Cuisine", regexp_replace("Cuisine Style", "'", "")).withColumn("Cuisine", trim(col("Cuisine"))).select("City","Cuisine")
count_df = (
    changed_df.groupBy("City", "Cuisine")
    .agg(count("*").alias("count"))
    .sort(col("City").asc(), col("count").desc())
).select("City", "Cuisine", "count")

count_df = count_df.orderBy(desc("count"))
count_df.show(truncate=False)


# AWSAcademy
count_df.write.csv("hdfs://%s:9000/assignment2/part1/output/question4/" % (hdfs_nn), header=True)
