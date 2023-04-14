import pandas as pd
import os

df = pd.read_csv("student_files/data/TA_restaurants_curated_cleaned.csv")


df_filtered = df.filter(df.Rating > 1.0)
if not os.mkdir("student_files/data/output"):
    os.mkdir("student_files/data/output")
df_filtered.to_csv("student_files/data/output/TA_restaurants_curated_cleaned.csv")

