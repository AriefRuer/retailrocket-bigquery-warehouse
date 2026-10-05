#!/usr/bin/env bash
# load_bronze.sh — Reproduce the initial RetailRocket load into BigQuery
#
# This script reflects the load: clean table names, correct dataset,
# header row skipped. It is NOT a byte-for-byte replay of the original GUI
# load, which had different table names and a different destination dataset.
#
#
# Prerequisites:
#   - gcloud CLI authenticated (gcloud auth login)
#   - GCS bucket gs://XXXXXXXXXXX_XXX/XXXXXXXXXXX/ populated
#   - Target dataset XXXXXXXXXXX_landing exists
#
# Usage: bash load_bronze.sh

set -euo pipefail

PROJECT="<GOOGLE PROJECT ID>"
DATASET="<YOUR DATASET FOR STAGING>"
BUCKET="<YOUR GOOGLE CLOUD STORAGE BUCKET>" 

echo "Loading RetailRocket CSVs into ${PROJECT}.${DATASET}..."

bq load \
  --source_format=CSV \
  --autodetect \
  --skip_leading_rows=1 \
  --write_disposition=WRITE_EMPTY \
  "${PROJECT}:${DATASET}.events" \
  "${BUCKET}/events.csv"

bq load \
  --source_format=CSV \
  --autodetect \
  --skip_leading_rows=1 \
  --write_disposition=WRITE_EMPTY \
  "${PROJECT}:${DATASET}.category_tree" \
  "${BUCKET}/category_tree.csv"

bq load \
  --source_format=CSV \
  --autodetect \
  --skip_leading_rows=1 \
  --write_disposition=WRITE_EMPTY \
  "${PROJECT}:${DATASET}.item_properties" \
  "${BUCKET}/item_properties_part*.csv"

echo ""
echo "Load complete. Run verify_counts.sql to validate."