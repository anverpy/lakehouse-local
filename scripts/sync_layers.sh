#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
AWS_ENDPOINT="${AWS_ENDPOINT:-http://localhost:4566}"

export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY
export AWS_DEFAULT_REGION

mkdir -p data/bronze data/silver data/gold

echo "Sync Bronze..."
aws --endpoint-url="$AWS_ENDPOINT" s3 sync s3://data-lake-bronze data/bronze --delete

echo "Sync Silver..."
aws --endpoint-url="$AWS_ENDPOINT" s3 sync s3://data-lake-silver data/silver --delete

echo "Sync Gold..."
aws --endpoint-url="$AWS_ENDPOINT" s3 sync s3://data-lake-gold data/gold --delete

echo "Listo: capas sincronizadas en data/{bronze,silver,gold}."