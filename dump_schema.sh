#!/bin/bash

# Load environment variables from .env file
set -a
# shellcheck source=/dev/null
[ -f .env ] && . .env
set +a

echo "Dumping schema..."
pg_dump "postgresql://${DB_USER}:${DB_PASS}@${DB_HOST}/${DB_NAME}?sslmode=require" --schema-only >schema.sql
echo "Done"
