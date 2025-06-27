import logging
import os

import boto3
from crhelper import CfnResource

logger = logging.getLogger(__name__)
helper = CfnResource()

try:
    dynamodb = boto3.resource("dynamodb")
    service_table = dynamodb.Table(os.environ.get("SERVICE_TABLE", ""))
# pylint: disable=broad-exception-caught
except Exception as e:
    helper.init_failure(e)


@helper.create
@helper.update
def create_update(event, context):
    props = event.get("ResourceProperties", {})
    storage_id = event.get("PhysicalResourceId", context.aws_request_id)
    service_table.put_item(
        Item={
            "record_type": "storage-backend",
            "id": storage_id,
            "bucket_name": props["bucket_name"],
            "provider": "aws",
            "region": props["region"],
            "store_product": "s3",
            "store_type": "http_object_store",
            "default_storage": props["default_storage"].lower() == "true",
        }
    )
    return storage_id


@helper.delete
def delete(event, context):
    storage_id = event.get("PhysicalResourceId", context.aws_request_id)
    service_table.delete_item(
        Key={
            "record_type": "storage-backend",
            "id": storage_id,
        }
    )
    return storage_id


def lambda_handler(event, context):
    print(event)
    helper(event, context)
