import json
import time

import pandas as pd
from kafka import KafkaProducer


# ---------------------------------------
# Configuration
# ---------------------------------------

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "financial-transactions"

FILE_PATH = "data/raw/PS_20174392719_1491204439457_log.csv"

# Number of transactions to send
MAX_RECORDS = 10000

# Delay between messages
DELAY = 0.01


# ---------------------------------------
# Create Kafka Producer
# ---------------------------------------

print("Connecting to Kafka...")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("Connected to Kafka successfully.")


# ---------------------------------------
# Read dataset in chunks
# ---------------------------------------

print("\nReading PaySim transactions...")

records_sent = 0

for chunk in pd.read_csv(
    FILE_PATH,
    chunksize=1000
):

    for _, row in chunk.iterrows():

        transaction = row.to_dict()

        producer.send(
            TOPIC_NAME,
            value=transaction
        )

        records_sent += 1

        if records_sent % 1000 == 0:
            print(
                f"Transactions sent: {records_sent}"
            )

        if records_sent >= MAX_RECORDS:
            break

        time.sleep(DELAY)

    if records_sent >= MAX_RECORDS:
        break


# ---------------------------------------
# Make sure all messages are sent
# ---------------------------------------

producer.flush()

producer.close()

print("\n========== PRODUCER COMPLETE ==========")
print(f"Total transactions sent: {records_sent}")