# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch all supported libraries
patch_all()

import boto3
import os
import json
import logging
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def handler(event, context):
    table = os.environ.get("TABLE_NAME")
    
    # Extract security context
    request_context = event.get("requestContext", {})
    request_id = context.request_id
    source_ip = request_context.get("identity", {}).get("sourceIp", "unknown")
    
    # Structured logging with security context
    log_entry = {
        "request_id": request_id,
        "source_ip": source_ip,
        "table_name": table,
    }
    
    try:
        if event.get("body"):
            item = json.loads(event["body"])
            log_entry["action"] = "put_item"
            log_entry["item_id"] = item.get("id")
            logger.info(json.dumps(log_entry))
            
            year = str(item["year"])
            title = str(item["title"])
            id = str(item["id"])
            dynamodb_client.put_item(
                TableName=table,
                Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
            )
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
        else:
            log_entry["action"] = "put_item_default"
            logger.info(json.dumps(log_entry))
            
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": "2012"},
                    "title": {"S": "The Amazing Spider-Man 2"},
                    "id": {"S": str(uuid.uuid4())},
                },
            )
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
    except Exception as e:
        log_entry["error"] = str(e)
        log_entry["status"] = "failed"
        logger.error(json.dumps(log_entry))
        raise
