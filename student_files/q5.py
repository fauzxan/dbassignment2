import sys 
from pyspark.sql import SparkSession
# you may add more import if you need to
from pyspark.sql.functions import  explode, count, col,  from_json, concat_ws, least, greatest


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW
df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))
)

json_parser = "array<struct<name:string>>"


#hint 
df = df.drop("crew")
df = df.withColumn(
    "actor1", explode(from_json(col("cast"), json_parser).getField("name"))
)
df = df.withColumn(
    "actor2", explode(from_json(col("cast"), json_parser).getField("name"))
)
df=df.select("movie_id","title","actor1","actor2").filter(col("actor1") != col("actor2"))


df = df.withColumn("helper", concat_ws(",", least("actor1", "actor2"), greatest("actor1", "actor2")))
new_df = df.dropDuplicates(["movie_id", "title", "helper"]).sort(col("helper").asc())

df_counter = (
    new_df.groupBy("helper")
    .agg(count("*").alias("count"))
    .filter(col("count") > 1)
)

final_df = (
    df_counter.join(new_df, ["helper"], "inner")
    .sort(col("helper").asc())
    .drop("helper", "count")

)
final_df.show()
rows = final_df.count()
print("\nrows:", rows)
final_df.write.option("header", True).mode("overwrite").parquet(
    "hdfs://%s:9000/assignment2/output/question5/" % (hdfs_nn)
)
