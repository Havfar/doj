#!/bin/bash

# Script to remove duplicate URLs from pdf-links.txt

INPUT_FILE="pdf-links.txt"
OUTPUT_FILE="pdf-links-unique.txt"
BACKUP_FILE="pdf-links-backup.txt"

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: $INPUT_FILE not found!"
    exit 1
fi

# Count original lines
ORIGINAL_COUNT=$(wc -l < "$INPUT_FILE")
echo "Original file has $ORIGINAL_COUNT URLs"

# Create backup
cp "$INPUT_FILE" "$BACKUP_FILE"
echo "Backup created: $BACKUP_FILE"

# Remove duplicates while preserving order (first occurrence kept)
awk '!seen[$0]++' "$INPUT_FILE" > "$OUTPUT_FILE"

# Count unique lines
UNIQUE_COUNT=$(wc -l < "$OUTPUT_FILE")
DUPLICATES_REMOVED=$((ORIGINAL_COUNT - UNIQUE_COUNT))

echo "Unique URLs: $UNIQUE_COUNT"
echo "Duplicates removed: $DUPLICATES_REMOVED"

# Ask user if they want to replace original file
read -p "Replace original file with deduplicated version? (y/n): " CONFIRM
if [ "$CONFIRM" = "y" ] || [ "$CONFIRM" = "Y" ]; then
    mv "$OUTPUT_FILE" "$INPUT_FILE"
    echo "Original file replaced with deduplicated version"
else
    echo "Deduplicated file saved as: $OUTPUT_FILE"
    echo "Original file unchanged"
fi

echo "Done!"
