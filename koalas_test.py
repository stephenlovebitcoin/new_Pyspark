import databricks.koalas as ks
import pandas as pd

students = [("James","Sales","NY",90,34,10),
    ("Michael","Sales","NY",86,56,20),
    ("Robert","Sales","CA",81,30,23),
    ("Maria","Finance","CA",90,24,23),
    ("Raman","Finance","CA",99,40,24),
    ("Scott","Finance","NY",83,36,19),
    ("Jen","Finance","NY",79,53,15),
    ("Jeff","Marketing","CA",80,25,18),
    ("Kumar","Marketing","NY",91,50,21)
  ]

schema = ["student_name","dept","province","a_score","age","b_score"]

df = ks.DataFrame(students)
df.columns =schema
print(df)


print("========================================")

df = pd.DataFrame(students)
df.columns =schema
print(df)
# 现在有了 Koalas，数据科学家可以从单个机器迁移到分布式环境，而无需学习新的框架。正如你在下面所看到的，只需替换一个包，就可以使用 Koalas 在 Spark 上扩展你的 pandas 代码。