import boto3
import json
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone

ENDPOINT = "http://localhost:4566"
REGION   = "us-east-1"
CREDS    = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}

BRONZE_BUCKET = "data-lake-bronze"
SILVER_BUCKET = "data-lake-silver"
PREFIX        = "ecommerce"

s3 = boto3.client("s3", endpoint_url=ENDPOINT, region_name=REGION, **CREDS)

SCHEMA = {
    "customer_id":       "int64",
    "age":               "int64",
    "gender":            "string",
    "annual_income":     "int64",
    "spending_score":    "int64",
    "membership_years":  "float64",
    "online_purchases":  "int64",
    "discount_usage":    "float64",
    "churn":             "int64",
    "_ingestion_ts":     "string",
    "_source":           "string",
}

CRITICAL_COLS = ["customer_id", "age", "annual_income", "churn"]

def list_bronze_files():
    resp = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=PREFIX)
    return [obj["Key"] for obj in resp.get("Contents", [])]

def read_bronze_file(key):
    obj  = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
    body = obj["Body"].read().decode("utf-8")
    return [json.loads(line) for line in body.strip().splitlines()]

def clean(df, source_key):
    initial_count = len(df)

    # Duplicados
    df = df.drop_duplicates(subset=["customer_id"])
    dupes = initial_count - len(df)
    if dupes:
        print(f"  Duplicados eliminados: {dupes}")

    # Nulls en columnas críticas
    null_mask = df[CRITICAL_COLS].isnull().any(axis=1)
    nulls = null_mask.sum()
    if nulls:
        print(f"  Filas con nulls críticos descartadas: {nulls}")
        df = df[~null_mask]

    # Castear tipos
    for col, dtype in SCHEMA.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)

    # Metadata Silver
    df["_processed_ts"]   = datetime.now(timezone.utc).isoformat()
    df["_bronze_source"]  = source_key

    return df

def upload_parquet(df):
    now = datetime.now(timezone.utc)
    key = (
        f"{PREFIX}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"silver_{now.strftime('%H%M%S')}.parquet"
    )
    table  = pa.Table.from_pandas(df)
    buf    = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    s3.put_object(Bucket=SILVER_BUCKET, Key=key, Body=buf.read())
    print(f"  Escrito → s3://{SILVER_BUCKET}/{key}")
    print(f"  Filas finales: {len(df)}")

if __name__ == "__main__":
    files = list_bronze_files()
    print(f"Ficheros Bronze encontrados: {len(files)}")

    all_records = []
    for key in files:
        print(f"  Leyendo {key}...")
        all_records.extend(read_bronze_file(key))

    print(f"Total registros leídos: {len(all_records)}")

    df = pd.DataFrame(all_records)
    print("Limpiando...")
    df = clean(df, source_key=str(files))
    upload_parquet(df)
    print("Silver completo.")
