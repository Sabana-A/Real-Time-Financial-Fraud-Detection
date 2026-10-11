import pandas as pd

file_path = "data/raw/PS_20174392719_1491204439457_log.csv"

print("Loading dataset...")

df = pd.read_csv(file_path)

print("\n========== FRAUD ANALYSIS ==========")

# 1. Fraud count
print("\nFraud Counts:")
print(df["isFraud"].value_counts())

# 2. Fraud percentage
fraud_percentage = df["isFraud"].mean() * 100

print("\nFraud Percentage:")
print(f"{fraud_percentage:.4f}%")

# 3. Fraud by transaction type
print("\nFraud by Transaction Type:")
print(
    df.groupby("type")["isFraud"]
    .agg(["count", "sum", "mean"])
    .sort_values("sum", ascending=False)
)

# 4. Average transaction amount
print("\nAverage Amount:")
print(
    df.groupby("isFraud")["amount"]
    .mean()
)

# 5. Average amount by transaction type
print("\nAverage Amount by Transaction Type:")
print(
    df.groupby("type")["amount"]
    .mean()
    .sort_values(ascending=False)
)

# 6. Fraud transaction amounts
fraud_df = df[df["isFraud"] == 1]

print("\nFraud Transaction Amount Statistics:")
print(fraud_df["amount"].describe())

print("\n========== ANALYSIS COMPLETE ==========")