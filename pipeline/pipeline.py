import sys
import pandas as pd

print("arguments", sys.argv)

month = int(sys.argv[1])

df = pd.DataFrame({"day": [1, 2], "num_passengers": [3, 4]})
df['month'] = month
print(df.head())

df.to_parquet(f"passengers_month_{month}.parquet", index=False) 
print(f"Running pipeline for month {month}")