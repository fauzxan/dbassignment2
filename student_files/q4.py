import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, regexp_replace, split, trim, col,count, desc


# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

#locally
# df = (
#     spark.read.option("header", True)
#     .option("inferSchema", True)
#     .option("delimiter", ",")
#     .option("quotes", '"')
#     .csv("data/TA_restaurants_curated_cleaned.csv")
# )

#AWS Academy
df = spark.read.option("header", True).option("inferSchema", True).csv("hdfs://{}:9000/assignment2/part1/input/".format(hdfs_nn))


df = df.withColumn("Cuisine Style", explode(split(col("Cuisine Style"), ", "))).select("City","Cuisine Style")
df = df.withColumn("Cuisine Style", explode(split(col("Cuisine Style"), ", "))).select("City","Cuisine Style")
#formatting
df = df.withColumn("Cuisine Style", regexp_replace("Cuisine Style", "\\[", "")).withColumn("Cuisine Style", regexp_replace("Cuisine Style", "\\]", ""))
trimmed_df= df.withColumn("Cuisine", regexp_replace("Cuisine Style", "'", "")).withColumn("Cuisine", trim(col("Cuisine"))).select("City","Cuisine")
count_df = (
    trimmed_df.groupBy("City", "Cuisine")
    .agg(count("*").alias("count"))
    .sort(col("City").asc(), col("count").desc())
).select("City", "Cuisine", "count")

count_df = count_df.orderBy(desc("count"))
count_df.show(truncate=False)

#local
# count_df.write.option("header", "true").csv("output/q4/")

# AWSAcademy
count_df.write.csv("hdfs://%s:9000/assignment2/part2/output/question4/" % (hdfs_nn), header=True)
