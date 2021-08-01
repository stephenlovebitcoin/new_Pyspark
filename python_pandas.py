import pandas as pd
import numpy as np



df = pd.DataFrame({"name": ['Alfred', 'Batman', 'Catwoman'],
                   "toy": [np.nan, 'Batmobile', 'Bullwhip'],
                   "born": [pd.NaT, pd.Timestamp("1940-04-25"),
                            pd.NaT]})


print(df)


df2 =df.dropna(how='any')
df3 = df.fillna('111')

print(df2)
print(df3)

# df.dropna(axis='columns')