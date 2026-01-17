#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME=postgres_source
FILE_URL="https://edu.postgrespro.com/demo-big-en.zip"

docker compose up -d "$SERVICE_NAME"

docker compose exec -u root -T "$SERVICE_NAME" bash -lc "
  apt-get update &&
  apt-get install -y wget gzip &&
  wget -O /tmp/demo-big-en.zip \"$FILE_URL\" &&
  zcat /tmp/demo-big-en.zip | psql -U postgres -d postgres
"

