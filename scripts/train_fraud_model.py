import pandas as pd
import numpy as np
import joblib

from sklearn.ensemble import IsolationForest
from sklearn.metrics import classification_report, confusion_matrix


# ---------------------------------------
# File paths
# ---------------------------------------

file_path = "data/processed/processed_transactions.csv"
model_path = "models/fraud_model.pkl"


print("Loading processed dataset...")

df = pd.read_csv(file_path)

print("Dataset shape:", df.shape)


# ---------------------------------------
# Features used by the model
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
# Separate normal and fraud transactions
# ---------------------------------------

normal_df = df[df["isFraud"] == 0]
fraud_df = df[df["isFraud"] == 1]

print("\nNormal transactions:", len(normal_df))
print("Fraud transactions:", len(fraud_df))


# ---------------------------------------
# Training data
# ---------------------------------------

X_normal = normal_df[features]

sample_size = 200000

if len(X_normal) > sample_size:
    X_train = X_normal.sample(
        n=sample_size,
        random_state=42
    )
else:
    X_train = X_normal


print("\nTraining samples:", len(X_train))


# ---------------------------------------
# Train Isolation Forest
# ---------------------------------------

print("\nTraining Isolation Forest...")

model = IsolationForest(
    n_estimators=200,
    contamination=0.01,
    random_state=42,
    n_jobs=-1
)

model.fit(X_train)

print("Model training completed.")


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
# Prediction
# ---------------------------------------

print("\nRunning predictions...")

predictions = model.predict(X_test)

# Isolation Forest:
#  1  = normal
# -1  = anomaly

y_pred = np.where(
    predictions == -1,
    1,
    0
)


# ---------------------------------------
# Evaluation
# ---------------------------------------

print("\n========== MODEL RESULTS ==========")

print("\nConfusion Matrix:")

print(
    confusion_matrix(
        y_true,
        y_pred
    )
)

print("\nClassification Report:")

print(
    classification_report(
        y_true,
        y_pred,
        digits=4
    )
)


# ---------------------------------------
# Save model
# ---------------------------------------

joblib.dump(
    model,
    model_path
)

print("\nModel saved successfully:")
print(model_path)

print("\n========== MODEL TRAINING COMPLETE ==========")