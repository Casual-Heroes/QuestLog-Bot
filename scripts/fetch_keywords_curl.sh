#!/bin/bash
# Fetch all IGDB keywords using curl
# Requires: TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET in .env

set -e

# Load .env file
if [ -f ../.env ]; then
    export $(grep -E '^(TWITCH_CLIENT_ID|TWITCH_CLIENT_SECRET)=' ../.env | xargs)
fi

if [ -z "$TWITCH_CLIENT_ID" ] || [ -z "$TWITCH_CLIENT_SECRET" ]; then
    echo "Error: TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET must be set"
    exit 1
fi

# Get OAuth token
echo "Getting Twitch OAuth token..."
TOKEN_RESPONSE=$(curl -s -X POST "https://id.twitch.tv/oauth2/token" \
    -d "client_id=$TWITCH_CLIENT_ID" \
    -d "client_secret=$TWITCH_CLIENT_SECRET" \
    -d "grant_type=client_credentials")

ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4)

if [ -z "$ACCESS_TOKEN" ]; then
    echo "Error: Failed to get access token"
    echo "$TOKEN_RESPONSE"
    exit 1
fi

echo "Got access token"

OUTPUT_FILE="${1:-igdb_keywords.txt}"
echo "Fetching all keywords to $OUTPUT_FILE..."

# Clear output file
> "$OUTPUT_FILE"

OFFSET=0
BATCH_SIZE=500
TOTAL=0

while true; do
    echo "Fetching offset $OFFSET..."

    RESPONSE=$(curl -s "https://api.igdb.com/v4/keywords" \
        -H "Client-ID: $TWITCH_CLIENT_ID" \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H "Accept: application/json" \
        -d "fields id, name, slug; sort name asc; limit $BATCH_SIZE; offset $OFFSET;")

    # Check if response is empty array
    if [ "$RESPONSE" = "[]" ]; then
        break
    fi

    # Count items in response
    COUNT=$(echo "$RESPONSE" | grep -o '"id":' | wc -l)

    if [ "$COUNT" -eq 0 ]; then
        break
    fi

    # Parse and append to file (simple extraction)
    echo "$RESPONSE" | grep -oP '"id":\s*\d+|"name":\s*"[^"]*"' | paste - - | \
        sed 's/"id":\s*//; s/"name":\s*"//; s/"$//' | \
        awk -F'\t' '{print $1 "\t" $2}' >> "$OUTPUT_FILE"

    TOTAL=$((TOTAL + COUNT))
    echo "  Got $COUNT keywords (total: $TOTAL)"

    if [ "$COUNT" -lt "$BATCH_SIZE" ]; then
        break
    fi

    OFFSET=$((OFFSET + BATCH_SIZE))

    # Rate limit - wait 0.3 seconds between requests
    sleep 0.3
done

echo ""
echo "Done! Saved $TOTAL keywords to $OUTPUT_FILE"
echo ""
echo "To search for specific keywords:"
echo "  grep -i 'souls' $OUTPUT_FILE"
