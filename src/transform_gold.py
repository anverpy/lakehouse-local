import boto3
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone

ENDPOINT = "http://localhost:4566"
REGION   = "us-east-1"
CREDS    = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}

SILVER_BUCKET = "data-lake-silver"
GOLD_BUCKET   = "data-lake-gold"
PREFIX        = "ecommerce"

s3 = boto3.client("s3", endpoint_url=ENDPOINT, region_name=REGION, **CREDS)

def read_silver():
    resp = s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=PREFIX)
    keys = [obj["Key"] for obj in resp.get("Contents", [])]
    frames = []
    for key in keys:
        obj = s3.get_object(Bucket=SILVER_BUCKET, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        frames.append(pq.read_table(buf).to_pandas())
    return pd.concat(frames, ignore_index=True)

def upload_gold(df, table_name):
    now = datetime.now(timezone.utc)
    key = (
        f"{table_name}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{table_name}_{now.strftime('%H%M%S')}.parquet"
    )
    table = pa.Table.from_pandas(df)
    buf   = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    s3.put_object(Bucket=GOLD_BUCKET, Key=key, Body=buf.read())
    print(f"  → s3://{GOLD_BUCKET}/{key}  ({len(df)} filas)")

def gold_segment_alto_valor(df):
    """Marketing: perfilado de segmentos de alto valor."""
    df["segmento_valor"] = pd.cut(
        df["spending_score"],
        bins=[0, 33, 66, 100],
        labels=["bajo", "medio", "alto"]
    )
    result = (
        df.groupby("segmento_valor", observed=True)
        .agg(
            num_clientes        =("customer_id",    "count"),
            avg_income          =("annual_income",  "mean"),
            avg_spending_score  =("spending_score", "mean"),
            avg_online_purchases=("online_purchases","mean"),
            churn_rate          =("churn",          "mean"),
        )
        .round(2)
        .reset_index()
    )
    return result

def gold_churn_prevention(df):
    """Retención: factores asociados al churn."""
    df["age_group"] = pd.cut(
        df["age"],
        bins=[0, 30, 45, 60, 100],
        labels=["18-30", "31-45", "46-60", "60+"]
    )
    result = (
        df.groupby(["age_group", "churn"], observed=True)
        .agg(
            num_clientes        =("customer_id",     "count"),
            avg_membership_years=("membership_years","mean"),
            avg_spending_score  =("spending_score",  "mean"),
            avg_online_purchases=("online_purchases", "mean"),
        )
        .round(2)
        .reset_index()
    )
    return result

def gold_discount_effectiveness(df):
    """Promociones: efectividad de descuentos por segmento."""
    df["discount_tier"] = pd.cut(
        df["discount_usage"],
        bins=[-0.01, 0.33, 0.66, 1.01],
        labels=["bajo", "medio", "alto"]
    )
    result = (
        df.groupby("discount_tier", observed=True)
        .agg(
            num_clientes        =("customer_id",    "count"),
            avg_spending_score  =("spending_score", "mean"),
            avg_online_purchases=("online_purchases","mean"),
            churn_rate          =("churn",          "mean"),
            avg_income          =("annual_income",  "mean"),
        )
        .round(2)
        .reset_index()
    )
    return result

def gold_generational_gaps(df):
    """Product / UX: brechas generacionales en comportamiento digital."""
    df["generacion"] = pd.cut(
        df["age"],
        bins=[0, 28, 44, 60, 100],
        labels=["GenZ", "Millennial", "GenX", "Boomer"]
    )
    result = (
        df.groupby("generacion", observed=True)
        .agg(
            num_clientes        =("customer_id",     "count"),
            avg_online_purchases=("online_purchases", "mean"),
            avg_discount_usage  =("discount_usage",   "mean"),
            avg_spending_score  =("spending_score",   "mean"),
            churn_rate          =("churn",            "mean"),
        )
        .round(2)
        .reset_index()
    )
    return result

def gold_clv_projection(df):
    """C-Level: proyección simplificada del Customer Lifetime Value."""
    df["clv_score"] = (
        df["annual_income"]      * 0.3 +
        df["spending_score"]     * 200 +
        df["membership_years"]   * 500 +
        df["online_purchases"]   * 50  -
        df["churn"]              * 3000
    ).round(2)

    df["clv_segment"] = pd.cut(
        df["clv_score"],
        bins=3,
        labels=["bajo", "medio", "alto"]
    )

    result = (
        df.groupby("clv_segment", observed=True)
        .agg(
            num_clientes        =("customer_id",    "count"),
            avg_clv_score       =("clv_score",      "mean"),
            avg_income          =("annual_income",  "mean"),
            avg_membership_years=("membership_years","mean"),
            churn_rate          =("churn",          "mean"),
        )
        .round(2)
        .reset_index()
    )
    return result

if __name__ == "__main__":
    print("Leyendo Silver...")
    df = read_silver()
    print(f"  {len(df)} registros cargados")

    tablas = {
        "marketing_segment_alto_valor" : gold_segment_alto_valor(df.copy()),
        "retencion_churn_prevention"   : gold_churn_prevention(df.copy()),
        "promociones_discount_effect"  : gold_discount_effectiveness(df.copy()),
        "product_generational_gaps"    : gold_generational_gaps(df.copy()),
        "clevel_clv_projection"        : gold_clv_projection(df.copy()),
    }

    print("Escribiendo Gold...")
    for nombre, tabla in tablas.items():
        print(f"\n[{nombre}]")
        print(tabla.to_string(index=False))
        upload_gold(tabla, nombre)

    print("\nGold completo.")
