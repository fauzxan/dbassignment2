import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import max
from pyspark.sql.functions import min

# you may add more import if you need to


# don't change this line
try:
    hdfs_nn = sys.argv[1].strip()
    print("\n\n\n\nSuccessfully retrieved system argument: ", hdfs_nn)
except Exception as e: 
    print(f"Error: {e}")
    print("<Usage>: spark-submit spark://<master>:7077 q1.py <hdfs_namenode>")
    sys.exit(1)
try:
    spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
except Exception as e:
    print(f"\n\nError while trying to create session: {e}")
    sys.exit(1)
# YOUR CODE GOES BELOW

df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

)
df = df.na.drop(how="any", subset=["Price Range", "Rating"])


best_df = (
    df.groupBy(["Price Range", "City"])
    .agg(max("Rating"))
    .withColumn("Rating", col("max(Rating)"))
    .drop("max(Rating)")
)

worst_df = (
    df.groupBy(["Price Range", "City"])
    .agg(min("Rating"))
    .withColumn("Rating", col("min(Rating)"))
    .drop("min(Rating)")
)

union_df = best_df.union(worst_df)
combined_df = union_df.join(df, on=["Price Range", "City", "Rating"], how="inner")

# Retain order in input into output
combined_df = (
    combined_df.dropDuplicates(["Price Range", "City", "Rating"])
    .select(
        "_c0",
        "Name",
        "City",
        "Cuisine Style",
        "Ranking",
        "Rating",
        "Price Range",
        "Number of Reviews",
        "Reviews",
        "URL_TA",
        "ID_TA",
    )
    .sort(col("City").asc(), col("Price Range").asc(), col("Rating").desc())
)

combined_df.show()
combined_df.write.csv(
    "hdfs://%s:9000/assignment2/part1/output/question2/" % (hdfs_nn), header=True
)
