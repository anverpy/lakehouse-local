import time
import json
import random
import logging
from pathlib import Path
from datetime import datetime

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError

STREAM_NAME = "ecommerce-stream"
CSV_PATH = Path(__file__).parent.parent / "data" / "raw" / "ecommerce_customer_data.csv"
DELAY_SECONDS = 0.1
BATCH_MODE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("producer")

kinesis = boto3.client(
    "kinesis",
    endpoint_url="http://localhost:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

def clean_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return value

def normalize_row(row: dict):
    normalized = {}
    for k, v in row.items():
        key = k.strip().lower().replace(" ", "_")
        normalized[key] = clean_value(v)
    normalized["_ingestion_ts"] = datetime.utcnow().isoformat()
    normalized["_source"] = "kaggle_ecommerce"
    return normalized

def send_record(data: dict):
    try:
        kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(data),
            PartitionKey=str(random.randint(1, 1000))
        )
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Error: {e}")

def run():
    logger.info("Cargando CSV...")
    df = pd.read_csv(CSV_PATH)
    logger.info(f"{len(df)} filas cargadas")
    for i, row in df.iterrows():
        event = normalize_row(row.to_dict())
        send_record(event)
        if i % 50 == 0:
            logger.info(f"Enviados {i} eventos")
        time.sleep(DELAY_SECONDS)
    logger.info("Streaming finalizado")

if __name__ == "__main__":
    run()
