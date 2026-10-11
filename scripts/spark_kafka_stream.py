import json
import joblib
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

from prometheus_client import Counter, start_http_server

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType
)


KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "financial-transactions"
MODEL_PATH = "models/fraud_model.pkl"


# --------------------------------------------------
# Prometheus metrics
# --------------------------------------------------

start_http_server(8000)

transactions_processed = Counter(
    "transactions_processed_total",
    "Total number of transactions processed"
)

fraud_detected = Counter(
    "fraud_detected_total",
    "Total number of fraud alerts detected"
)

batches_processed = Counter(
    "batches_processed_total",
    "Total number of Spark batches processed"
)

print("Prometheus metrics server started on port 8000.")


# --------------------------------------------------
# 1. Start Spark
# --------------------------------------------------


spark = (
    SparkSession.builder
    .appName("FinancialFraudDetection")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark started successfully.")
print("Loading fraud detection model...")


# --------------------------------------------------
# 2. Load Isolation Forest model
# --------------------------------------------------

model = joblib.load(MODEL_PATH)

print("Fraud model loaded successfully.")


# --------------------------------------------------
# MongoDB connection
# --------------------------------------------------

mongo_client = MongoClient("mongodb://localhost:27017")
mongo_db = mongo_client["fraud_detection"]
fraud_collection = mongo_db["fraud_alerts"]

print("Connected to MongoDB successfully.")

print("Connecting to Kafka...")


# --------------------------------------------------
# 3. Define Kafka transaction schema
# --------------------------------------------------

schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True)
])


# --------------------------------------------------
# 4. Read transactions from Kafka
# --------------------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .load()
)


transactions = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)


# --------------------------------------------------
# 5. Fraud detection function
# --------------------------------------------------

def detect_fraud(batch_df, batch_id):

    if batch_df.count() == 0:
        return

    print(f"\n========== BATCH {batch_id} ==========")

    pdf = batch_df.toPandas()

    # --------------------------------------------------
    # Prometheus counters
    # --------------------------------------------------

    transactions_processed.inc(len(pdf))
    batches_processed.inc()

    # --------------------------------------------------
    # Create the same features used during model training
    # --------------------------------------------------

    pdf["balance_change_orig"] = (
        pdf["oldbalanceOrg"] - pdf["newbalanceOrig"]
    )

    pdf["balance_change_dest"] = (
        pdf["newbalanceDest"] - pdf["oldbalanceDest"]
    )

    pdf["amount_to_old_balance"] = (
        pdf["amount"] / (pdf["oldbalanceOrg"] + 1)
    )

    pdf["transaction_hour"] = pdf["step"] % 24


    # --------------------------------------------------
    # Features used by Isolation Forest
    # --------------------------------------------------

    feature_columns = [
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

    X = pdf[feature_columns]


    # --------------------------------------------------
    # Predict
    # --------------------------------------------------

    predictions = model.predict(X)

    # Isolation Forest:
    # -1 = anomaly
    #  1 = normal

    pdf["prediction"] = predictions

    pdf["fraud_prediction"] = pdf["prediction"].apply(
        lambda x: "FRAUD" if x == -1 else "NORMAL"
    )


    # --------------------------------------------------
    # Display detected fraud
    # --------------------------------------------------

    fraud_df = pdf[pdf["fraud_prediction"] == "FRAUD"]


    if len(fraud_df) > 0:

        print("\n🚨 FRAUD TRANSACTIONS DETECTED 🚨")

        # Prometheus fraud counter
        fraud_detected.inc(len(fraud_df))


        # --------------------------------------------------
        # Save fraud alerts to MongoDB
        # --------------------------------------------------

        fraud_records = fraud_df[
            [
                "step",
                "type",
                "amount",
                "nameOrig",
                "nameDest",
                "fraud_prediction"
            ]
        ].to_dict("records")

        detected_time = datetime.now()

        for record in fraud_records:
            record["batch_id"] = int(batch_id)
            record["detected_at"] = detected_time

        fraud_collection.insert_many(fraud_records)

        print(
            f"Saved {len(fraud_records)} fraud alerts to MongoDB."
        )


        # --------------------------------------------------
        # Display fraud transactions
        # --------------------------------------------------

        print(
            fraud_df[
                [
                    "step",
                    "type",
                    "amount",
                    "nameOrig",
                    "nameDest",
                    "fraud_prediction"
                ]
            ].to_string(index=False)
        )

        print(
            f"\nFraud alerts in this batch: {len(fraud_df)}"
        )


    else:

        print("No fraud detected in this batch.")



# --------------------------------------------------
# 6. Start Spark Streaming
# --------------------------------------------------

query = (
    transactions.writeStream
    .foreachBatch(detect_fraud)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "checkpoints/fraud-stream"
    )
    .start()
)

print("Spark is now detecting fraud from Kafka transactions...")

query.awaitTermination()