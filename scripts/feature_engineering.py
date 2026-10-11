import pandas as pd
import os

file_path = "data/raw/PS_20174392719_1491204439457_log.csv"

output_dir = "data/processed"
output_file = os.path.join(output_dir, "processed_transactions.csv")

os.makedirs(output_dir, exist_ok=True)

print("Loading dataset...")

df = pd.read_csv(file_path)

print("Original Shape:", df.shape)

# --------------------------------
# Feature 1: Original balance change
# --------------------------------
df["balance_change_orig"] = (
    df["oldbalanceOrg"] - df["newbalanceOrig"]
)

# --------------------------------
# Feature 2: Destination balance change
# --------------------------------
df["balance_change_dest"] = (
    df["newbalanceDest"] - df["oldbalanceDest"]
)

# --------------------------------
# Feature 3: Amount / old balance
# --------------------------------
df["amount_to_old_balance"] = (
    df["amount"] / (df["oldbalanceOrg"] + 1)
)

# --------------------------------
# Feature 4: Transaction hour
# PaySim step represents time
# 24 steps = approximately one day
# --------------------------------
df["transaction_hour"] = df["step"] % 24

# --------------------------------
# Select useful columns
# --------------------------------
features = [
    "step",
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "isFlaggedFraud",
    "balance_change_orig",
    "balance_change_dest",
    "amount_to_old_balance",
    "transaction_hour",
    "isFraud"
]

df = df[features]

print("\nProcessed Shape:", df.shape)

print("\nNew Features:")
print(df.columns.tolist())

print("\nSaving processed dataset...")

df.to_csv(output_file, index=False)

print("\nSaved successfully:")
print(output_file)

print("\n========== FEATURE ENGINEERING COMPLETE ==========")