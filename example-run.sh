#!/bin/bash

# Example configuration script for magento2-media-cleaner
# Copy this file to config.sh and update with your settings

# Database Configuration
DB_HOST="localhost"
DB_PORT="3306"
DB_NAME="magento2"
DB_USER="root"
DB_PASS="password"
DB_PREFIX=""  # Leave empty if no prefix, otherwise e.g., "m2_"

# Magento 2 Media Path (absolute path to pub/media/catalog/product)
MEDIA_PATH="/var/www/html/pub/media/catalog/product"

# Performance Configuration
WORKERS=10  # Number of parallel workers for file scanning

# Build the application (if needed)
# go build -o magento2-media-cleaner

# Example 1: Just show statistics (no changes)
echo "=== Getting statistics ==="
./magento2-media-cleaner \
  -db-host="$DB_HOST" \
  -db-port="$DB_PORT" \
  -db-name="$DB_NAME" \
  -db-user="$DB_USER" \
  -db-pass="$DB_PASS" \
  -db-prefix="$DB_PREFIX" \
  -media-path="$MEDIA_PATH" \
  -workers="$WORKERS"

# Example 2: List unused files
echo ""
echo "=== Listing unused files ==="
./magento2-media-cleaner -u \
  -db-host="$DB_HOST" \
  -db-port="$DB_PORT" \
  -db-name="$DB_NAME" \
  -db-user="$DB_USER" \
  -db-pass="$DB_PASS" \
  -db-prefix="$DB_PREFIX" \
  -media-path="$MEDIA_PATH" \
  -workers="$WORKERS"

# Example 3: List missing files
# ./magento2-media-cleaner -m \
#   -db-host="$DB_HOST" \
#   -db-name="$DB_NAME" \
#   -db-user="$DB_USER" \
#   -db-pass="$DB_PASS" \
#   -media-path="$MEDIA_PATH"

# Example 4: List duplicate files
# ./magento2-media-cleaner -d \
#   -db-host="$DB_HOST" \
#   -db-name="$DB_NAME" \
#   -db-user="$DB_USER" \
#   -db-pass="$DB_PASS" \
#   -media-path="$MEDIA_PATH"

# Example 5: Remove unused files (CAUTION: This deletes files!)
# ./magento2-media-cleaner -r \
#   -db-host="$DB_HOST" \
#   -db-name="$DB_NAME" \
#   -db-user="$DB_USER" \
#   -db-pass="$DB_PASS" \
#   -media-path="$MEDIA_PATH"

# Example 6: Remove orphaned database entries
# ./magento2-media-cleaner -o \
#   -db-host="$DB_HOST" \
#   -db-name="$DB_NAME" \
#   -db-user="$DB_USER" \
#   -db-pass="$DB_PASS" \
#   -media-path="$MEDIA_PATH"

# Example 7: Remove duplicates (CAUTION: This deletes files and updates DB!)
# ./magento2-media-cleaner -x \
#   -db-host="$DB_HOST" \
#   -db-name="$DB_NAME" \
#   -db-user="$DB_USER" \
#   -db-pass="$DB_PASS" \
#   -media-path="$MEDIA_PATH"

# Example 8: Full cleanup (CAUTION: This performs all cleanup operations!)
# ./magento2-media-cleaner -r -o -x \
#   -db-host="$DB_HOST" \
#   -db-name="$DB_NAME" \
#   -db-user="$DB_USER" \
#   -db-pass="$DB_PASS" \
#   -media-path="$MEDIA_PATH"
