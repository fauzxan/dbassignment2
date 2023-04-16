import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, regexp_replace, split, trim, col, arrays_zip

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()

df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
)

split_cols = split(df["Reviews"], "\\], \\[")

df = df.withColumn("review", split_cols.getItem(0)).withColumn("date", split_cols.getItem(1))

# helper_df = df.withColumn("review", split(col("review"), "\\', \\'")).withColumn(
#     "date", split(col("date"), "\\', \\'")
# ).select("ID_TA","review","date")
helper_df = df.withColumn("review", split(trim(split_cols.getItem(0)), "', '")).withColumn(
    "date", split(trim(split_cols.getItem(1)), "', '")
)


helper_df.select("review").show(truncate=False)

# # Combine the "review" and "date" arrays into a new array column.
new_df = helper_df.withColumn("helper", arrays_zip("review", "date"))

#new_df.select("helper").show(truncate=False)

# # # Explode the "new" array column into multiple rows.
new_df = new_df.withColumn("helper", explode("helper"))
final_df= new_df.select("ID_TA", col("helper.review").alias("review"), col("helper.date").alias("date"))
final_df.select("review").show(truncate=False)


# # Remove single quotes and brackets from the "review" and "date" columns.
final_df = final_df.withColumn("review", regexp_replace("review", "'", "")).withColumn(
    "date", regexp_replace("date", "'", "")).withColumn("review", regexp_replace("review", "\\[", "")).withColumn("date", regexp_replace("date", "\\]", ""))

final_df = final_df.withColumn("review", trim(final_df.review)).withColumn(
   "date", trim(final_df.date) )


final_df.show()
rows = df.count()

final_df.write.csv("hdfs://%s:9000/assignment2/part1/output/question3/" % (hdfs_nn), header=True)
