import pandas as pd

file_path = "data/raw/PS_20174392719_1491204439457_log.csv"

print("Loading dataset...")

df = pd.read_csv(file_path)

print("\nDataset Shape:")
print(df.shape)

print("\nColumns:")
print(df.columns.tolist())

print("\nFirst 5 Rows:")
print(df.head())

print("\nData Types:")
print(df.dtypes)

print("\nMissing Values:")
print(df.isnull().sum())

print("\nTransaction Type Counts:")
print(df["type"].value_counts())

print("\nFraud Counts:")
print(df["isFraud"].value_counts())

print("\nFraud Percentage:")
print(df["isFraud"].mean() * 100)