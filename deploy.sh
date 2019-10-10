#!/bin/bash

FUNCTION_NAME=$(cat config/function_name)
ALLOWED_ENDPOINTS=$(cat config/allowed_endpoints)
RANGE_UPPER_LIMIT=$(cat config/range_upper_limit)

set -e
set -x

# Run tests
pytest

# Get latest code version
#VERSION=$(git for-each-ref refs/tags\
#	      --sort=-taggerdate \
#	      --format='%(refname)' \
#	      --count=1 | cut -c 12-)
VERSION=11

# Check if lambda exists
set +e
set +x
aws lambda get-function --function-name $FUNCTION_NAME:Version$VERSION &> /dev/null

if [ $? -ne 0 ]
then
    echo "Version $VERSION not found"
    # Deploy new lambda version
    zip clio_lite.zip clio_lite.py
    PYCODE_="import sys, json; print(json.load(sys.stdin)['Version'])"
    FUNCTION_VERSION=$(aws lambda update-function-code \
			   --function-name $FUNCTION_NAME \
			   --zip-file fileb://clio_lite.zip \
			   --publish | python -c "$PYCODE_")
    rm clio_lite.zip
    
    # Set the new alias
    echo "Created lambda function with pseudoversion $FUNCTION_VERSION"
    aws lambda create-alias \
	--function-name $FUNCTION_NAME \
	--name Version$VERSION \
	--function-version=$FUNCTION_VERSION

    # Pick up the allowed endpoints
    aws lambda update-function-configuration \
	--function-name $FUNCTION_NAME \
	--environment Variables="{ALLOWED_ENDPOINTS=$ALLOWED_ENDPOINTS,RANGE_UPPER_LIMIT=$RANGE_UPPER_LIMIT}"
else
    echo "Nothing to do for version $VERSION"
fi
