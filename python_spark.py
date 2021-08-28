from datetime import timedelta,date
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import pyspark.sql.types as T

spark = SparkSession.builder.appName('test').getOrCreate()

columns = ["ID","name","gender","language","users_count","update_time"]

data = [(123,"James","M","Java,123", 20000,"1991-01-11 20:01:28"),
        (456,"Helen","F","Python,456", 100000,"2021-02-11 20:01:28"),
        (789,"Jack", "M","Scala,789", 3000,"2021-03-11 20:01:28"),
        (11789,"Lily", "F","C,11789",0,"2018-03-11 20:01:28")]

df  = spark.createDataFrame(data=data,schema=columns)
df = df.withColumn("gender", F.when(F.col('gender')=="F","Female")\
    .when(F.col("gender")=="M","Male")\
    .when(F.col("gender").isNull(),"")\
    .otherwise(df.gender))

partition_column_list = ["name","language"]

df = df.select(*columns,
          F.row_number().over(Window.partitionBy(*partition_column_list).orderBy(F.col("update_time"))).alias("rowNum"))

df.show()


days=date.today()-timedelta(days=170)
print(days)


df = df.select(
*columns
,F.date_format(df["update_time"].cast(T.DateType()),'yyyy-MM-dd').alias("update_date")
,F.current_date().alias("current_date")
)


expression="^J"

df = df.filter(F.col("name").rlike(expression))\
    .filter(F.col("update_date")==F.date_sub(F.col("current_date"),170))

df.show()

#split the language column
df = df.withColumn("language",F.explode(F.split(F.col("language"),',')))

df.show()

