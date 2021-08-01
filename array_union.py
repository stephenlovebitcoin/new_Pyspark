import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()
from pyspark.sql.types import NullType

data = [('James',None,'Sm-ith','1991-04-01'),
  ('Michael','Ro-se','','2000-05-19'),
  ('Robert',None,'Will-iams','1978-09-05'),
  ('Maria','An-ne','Jo-nes','1967-12-01'),
  ('Jen','Ma-ry','Br-own','1980-02-17')
]


df = spark.createDataFrame(data=data, schema = ['firstname','middlename','lastname','dob'])
df.printSchema()
df.show()

df2 = df.withColumn('mycolumn',F.coalesce(F.array_union(F.split(F.col('middlename'),'-'),F.split(F.col('dob'),'-'))\
                                          ,F.split(F.col('dob'),'-')))
df2.show()


#
# df2 = df.withColumn(F.col('knownLanguages'),F.split(F.col('knownLanguages'),','))
# df2.show()
#
# df2 = df.withColumn('dob',F.split(F.col('dob'),'-'))\
#         .withColumn('lastname',F.split(F.col('lastname'),'-'))\
#         .withColumn('lastname',F.array_union(F.col('lastname'),F.col('dob')))
#
# df2.show()
#
#
# # #
# df3 = df.withColumn('mycolumn',F.coalesce(F.array_union(F.split(F.col('middlename'),'-'),F.split(F.col('dob'),'-'))\
#                                           ,F.split(F.col('dob'),'-')))
# df3.show()