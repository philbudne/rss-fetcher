#!/bin/sh

# cloned from web-search
if [ "x$AIRTABLE_API_KEY" != x ]; then
    # extract version from fetcher
    VERSION=$(python -c 'from fetcher.version import VERSION; print(VERSION)')

    echo TEMP-- should all be supplied via Dokku config:
    echo "AIRTABLE_API_KEY=$AIRTABLE_API_KEY"
    echo "AIRTABLE_BASE_ID=$AIRTABLE_BASE_ID"
    echo "AIRTABLE_ENV=$AIRTABLE_ENV"
    echo "AIRTABLE_HARDWARE=$AIRTABLE_HARDWARE"
    echo "AIRTABLE_NAME=$AIRTABLE_NAME"
    echo "VERSION=$VERSION"

    # AIRTABLE_API_KEY used w/o change
    MEAG_BASE_ID=$AIRTABLE_BASE_ID \
		python -m mc-manage.airtable-deployment-update \
		    --codebase 'rss-fetcher' \
		    --env "$AIRTABLE_ENV" \
		    --hardware "$AIRTABLE_HARDWARE" \
		    --name "$AIRTABLE_NAME" \
		    --version "$VERSION"
fi
