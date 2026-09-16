import pandas as pd
summary = pd.read_parquet("data/clean/jobs_summary.parquet")
print(summary)

jobs = pd.read_parquet("data/clean/jobs.parquet")
print(jobs["city"].value_counts())
print(jobs["title"].str.upper().value_counts().head(10))
