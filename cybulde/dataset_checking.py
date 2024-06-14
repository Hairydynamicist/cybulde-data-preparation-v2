import pandas as pd

test_df = pd.read_parquet("https://storage.cloud.google.com/jmcybulde/data/processed/default_run/test.parquet")

print(test_df.shape)