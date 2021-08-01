import time
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("data skew").\
config("spark.driver.bindAddress","localhost").\
config("spark.ui.port","4050"). \
config("spark.sql.shuffle.partitions",3). \
config("spark.sql.autoBroadcastJoinThreshold",-1). \
    getOrCreate()


df1 = spark.read.csv("C:\\test\\student.csv",header=True)
df2 = spark.read.csv("C:\\test\\score.csv",header=True)


df1.join(df2,df1['id']==df2['id']).show(100)#.select(df2["id"]).groupBy(df2["id"]).agg(F.count("*"))

##解决方案?

df1.join(df2,df1['id']==df2['id']).show(100)
F.concat(df1.id, F.lit("_"), F.lit(F.floor(F.rand(123) * 10)))

#
#
# df1 = [("x", "bc"),
#     ("x", "ce"),
#     ("x", "ab"),
#     ("x", "ef"),
#     ("x", "gh"),
#     ("y", "hk"),
#     ("z", "jk")]
#
# df2 = [("x", "gkl"),
#     ("y", "nmb"),
#     ("z", "qwe")]
#
# df1 = spark.createDataFrame(data=df1, schema = ['id','name'])
# df2 = spark.createDataFrame(data=df2, schema = ['id','name'])
#
#
# df1 = df1.withColumn('id', F.concat(
#     df1.id, F.lit("_"), F.lit(F.floor(F.rand(123) * 10))))
#
#
# df1.join(df2,df1['id']==df2['id']).show(100)#.select(df2["id"]).groupBy(df2["id"]).agg(F.count("*"))
#







time.sleep(1000)