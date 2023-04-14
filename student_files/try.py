import pandas as pd

df = pd.read_csv("student_files/data/TA_restaurants_curated_cleaned.csv")


print(df.filter(df.Rating > 1.0))