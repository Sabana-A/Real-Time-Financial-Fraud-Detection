import pandas as pd
import numpy as np

from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_score, recall_score, f1_score


# ---------------------------------------
# Load processed dataset
# ---------------------------------------

file_path = "data/processed/processed_transactions.csv"

print("Loading dataset...")

df = pd.read_csv(file_path)

print("Dataset shape:", df.shape)


# ---------------------------------------
# Features
# ---------------------------------------

features = [
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "isFlaggedFraud",
    "balance_change_orig",
    "balance_change_dest",
    "amount_to_old_balance",
    "transaction_hour"
]


# ---------------------------------------
# Separate normal and fraud
# ---------------------------------------

normal_df = df[df["isFraud"] == 0]
fraud_df = df[df["isFraud"] == 1]


# ---------------------------------------
# Training data
# ---------------------------------------

X_normal = normal_df[features]

X_train = X_normal.sample(
    n=200000,
    random_state=42
)


# ---------------------------------------
# Test data
# ---------------------------------------

test_normal = normal_df.sample(
    n=20000,
    random_state=42
)

test_df = pd.concat(
    [test_normal, fraud_df],
    ignore_index=True
)

X_test = test_df[features]
y_true = test_df["isFraud"]


# ---------------------------------------
# Test different contamination values
# ---------------------------------------

contamination_values = [
    0.001,
    0.002,
    0.005,
    0.01
]


print("\n========== MODEL TUNING ==========")


for contamination in contamination_values:

    print("\n--------------------------------")
    print("Contamination:", contamination)
    print("--------------------------------")

    model = IsolationForest(
        n_estimators=100,
        contamination=contamination,
        random_state=42,
        n_jobs=-1
    )

    model.fit(X_train)

    predictions = model.predict(X_test)

    y_pred = np.where(
        predictions == -1,
        1,
        0
    )

    precision = precision_score(
        y_true,
        y_pred,
        zero_division=0
    )

    recall = recall_score(
        y_true,
        y_pred,
        zero_division=0
    )

    f1 = f1_score(
        y_true,
        y_pred,
        zero_division=0
    )

    print("Precision:", round(precision, 4))
    print("Recall:", round(recall, 4))
    print("F1 Score:", round(f1, 4))


print("\n========== TUNING COMPLETE ==========")