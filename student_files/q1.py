import sys
from pyspark.sql import SparkSession
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
    spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
except Exception as e:
    print(f"\n\nError while trying to create session: {e}")
    sys.exit(1)
# YOUR CODE GOES BELOW
df = spark.read.option("header",True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/")
df.printSchema()


df_filtered = df.filter(df.Reviews.isNotNull()).filter(df.Rating.isNotNull & df.Rating > 1.0)
df_filtered.printSchema()

df_filtered.write.csv(f"hdfs://{hdfs_nn}:9000/assignment2/part1/output/", header=True)
# Print the filtered rows
df_filtered.show()