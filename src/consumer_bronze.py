import boto3
import json
from datetime import datetime, timezone

ENDPOINT = "http://localhost:4566"
REGION   = "us-east-1"
CREDS    = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}

STREAM_NAME  = "ecommerce-stream"
BUCKET       = "data-lake-bronze"
PREFIX       = "ecommerce"

kinesis = boto3.client("kinesis", endpoint_url=ENDPOINT, region_name=REGION, **CREDS)
s3      = boto3.client("s3",      endpoint_url=ENDPOINT, region_name=REGION, **CREDS)

def get_all_records():
    shard_id = kinesis.list_shards(StreamName=STREAM_NAME)["Shards"][0]["ShardId"]
    iterator = kinesis.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON"
    )["ShardIterator"]

    records = []
    while iterator:
        resp = kinesis.get_records(ShardIterator=iterator, Limit=100)
        for r in resp["Records"]:
            records.append(json.loads(r["Data"].decode("utf-8")))
        iterator = resp.get("NextShardIterator")
        if not resp["Records"] and resp["MillisBehindLatest"] == 0:
            break
    return records

def upload_to_bronze(records):
    now  = datetime.now(timezone.utc)
    key  = (
        f"{PREFIX}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"batch_{now.strftime('%H%M%S')}.json"
    )
    body = "\n".join(json.dumps(r) for r in records)   # JSON lines
    s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"))
    print(f"Subidos {len(records)} registros → s3://{BUCKET}/{key}")

if __name__ == "__main__":
    print("Leyendo Kinesis...")
    records = get_all_records()
    print(f"  Registros leídos: {len(records)}")
    if records:
        upload_to_bronze(records)
    else:
        print("  No hay registros en el stream.")
