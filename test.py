import pyspark
from pyspark.sql import SparkSession
from  pyspark.sql.functions import *

spark=SparkSession.builder.appName("stringoperations").getOrCreate()
df = spark.read.csv("C:\\test\\student.csv")

df2= df.withColumn("filename", input_file_name())
df2.withColumn("filename", substring("filename",17,100)).show()