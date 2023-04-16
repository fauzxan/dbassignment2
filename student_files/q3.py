import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
import ast

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header", True).option("inferSchema", True).csv("hdfs://{}:9000/assignment2/part1/input/".format(hdfs_nn))

df = df.select(fn.col('ID_TA'), fn.col('Reviews'))

# Converts the "[['Just like home', 'A Warm Welcome to Wintry Amsterdam'], ['01/03/2018', '01/01/2018']]" to an Array
def MaketoArray(intput_str):
    array = ast.literal_eval(input_str)
    return array
# Using RDD to utilise map functionality 
df = df.rdd
df = df.map(lambda cols: ( cols[0],MaketoArray(cols[1])[0], MaketoArray(cols[1])[1] )) #[0] is ID_TA, The second one would be the list of reviews, the third one would be the list of dates 

df = df.toDF(["ID_TA", "review", "date"])

df = df.withColumn('new', fn.arrays_zip("review", "date")).withColumn('new', fn.explode('new')).select("ID_TA" , fn.col("new.review"), fn.col("new.date")) # review and  date columns are represented as lists of strings (zip) following which each review and its corresponding date are represented as separate rows (explode).
    
# Sanity Check
df.show()

df.write.csv("hdfs://%s:9000/assignment2/part1/output/question3/" % (hdfs_nn), header=True)