优化之前的代码



from pyspark.sql import SparkSession

from pyspark.sql.functions import col, struct, array

from pyspark.sql import functions as F

from pyspark.sql.types import StructType,ArrayType,IntegerType,StructField,StringType

# 初始化Spark会话

spark = SparkSession.builder.getOrCreate()





data = [

    (10, [{"Dep": 10, "ABC": 1}, {"Dep": 10, "ABC": 1}]),

    (20, [{"Dep": 20, "ABC": 1}, {"Dep": 20, "ABC": 1}]),

    (30, [{"Dep": 30, "ABC": 1}, {"Dep": 30, "ABC": 1}]),

    (40, [{"Dep": 40, "ABC": 1}, {"Dep": 40, "ABC": 1}])

  ]

myschema = StructType(

[

    StructField("id", IntegerType(), True),

    StructField("values",

                ArrayType(

                    StructType([

                        StructField("Dep", StringType(), True),

                        StructField("ABC", StringType(), True)

                    ])

    ))

]

)

df = spark.createDataFrame(data=data, schema=myschema)

df.printSchema()

df.show(10, False)





df = df.withColumn('values', F.transform('values', lambda x: x.withField('id', F.lit(None))))\

     .withColumn('values', F.transform('values', lambda x: x.withField('name', F.lit(''))))\

     .withColumn('values', F.transform('values', lambda x: x.withField('age', F.lit(32).cast('int'))))\



df.show(10, False)









优化之后的代码For spark version >= 3.1, you can use the transform function and withField method to achieve this.



from pyspark.sql import SparkSession

from pyspark.sql.functions import col, struct, array

from pyspark.sql import functions as F

from pyspark.sql.types import StructType, ArrayType, IntegerType, StructField, StringType



# 初始化Spark会话

spark = SparkSession.builder.getOrCreate()



data = [

    (10, [{"Dep": 10, "ABC": 1}, {"Dep": 10, "ABC": 1}]),

    (20, [{"Dep": 20, "ABC": 1}, {"Dep": 20, "ABC": 1}]),

    (30, [{"Dep": 30, "ABC": 1}, {"Dep": 30, "ABC": 1}]),

    (40, [{"Dep": 40, "ABC": 1}, {"Dep": 40, "ABC": 1}])

]



myschema = StructType([

    StructField("id", IntegerType(), True),

    StructField("values",

                ArrayType(

                    StructType([

                        StructField("Dep", StringType(), True),

                        StructField("ABC", StringType(), True)

                    ])

                )

            )

        ]

    )



df = spark.createDataFrame(data=data, schema=myschema)

df.printSchema()

df.show(10, False)



# 添加判断

values_element_type = df.schema["values"].dataType.elementType

fields_to_check = ["id", "name", "age"]

transformed_df = df

for field in fields_to_check:

    if isinstance(values_element_type, StructType) and field not in values_element_type.fieldNames():

        transformed_df = transformed_df.withColumn('values', F.transform('values', lambda x: x.withField(field, F.lit(None).cast('string'))))



transformed_df.show(10, False)

transformed_df.printSchema()
