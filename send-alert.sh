#!/bin/bash
set -e

# Load .env file in the same directory
set -o allexport
source "$(dirname "$0")/.env"
set +o allexport

curl -X POST "$API_INVOKE_URL/alert" \
  -H 'Content-Type: application/json' \
  -d "{
    \"device_id\": \"DEV-A100\",
    \"error_code\": \"CRITICAL_OVERHEAT\",
    \"details\": \"CPU temperature exceeded 100C threshold.\",
    \"alert_timestamp\": \"2025-10-03T09:30:00Z\"
  }"
