import pandas as pd

df = pd.read_csv("student_files/data/TA_restaurants_curated_cleaned.csv")


df_filtered = df.filter(df.Rating > 1.0)



