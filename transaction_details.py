from pyspark.sql import SparkSession
from pyspark.sql.functions import  when,col,sum
import time


spark=SparkSession.builder.appName("stringoperations").getOrCreate()
df = spark.read.csv("C:\\test\\dataset\\transactions_details.csv",header=True)


df2 = df.withColumn("amt_chk",when(col("debit_credit")=='debit',-1*col("amt")).otherwise(col("amt")))
df2.show()

df3 = df2.groupby("custom_ID").agg(sum("amt_chk").alias("total_balance"))
df3.show()

print("运行耗时",time.process_time())#运行耗时 0.421875
#
# #
df4 = df.groupby("custom_ID").pivot("debit_credit").agg(sum("amt"))
df4.show()
df5 = df4.withColumn("total_balance",col("credit")-col("debit")).drop("credit","debit")
df5.show()
print("运行耗时",time.process_time())#运行耗时 0.34375
