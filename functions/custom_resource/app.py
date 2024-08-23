import logging
import os

import boto3
from crhelper import CfnResource

logger = logging.getLogger(__name__)
helper = CfnResource()

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE"])
solution_version = os.environ["SOLUTION_VERSION"]
api_id = os.environ["API_ID"]

try:
    pass
# pylint: disable=broad-exception-caught
except Exception as e:
    helper.init_failure(e)


@helper.delete
# pylint: disable=unused-argument
def delete(event, context):
    logger.info(f'{event["RequestType"]}...')
    table.delete_item(
        Key={
            "record_type": "service",
            "id": "1",
        }
    )


@helper.create
@helper.update
# pylint: disable=unused-argument
def create(event, context):
    logger.info(f'{event["RequestType"]}...')
    table.put_item(
        Item={
            "record_type": "service",
            "id": "1",
            "information": {
                "type": "urn:x-tams:service.example",
                "api_version": f"{get_api_version()}",
                "service_version": f"aws.{solution_version}",
                "media_store": {"type": "http_object_store"},
            },
        }
    )


def get_api_version():
    agw = boto3.client("apigateway")
    api = agw.get_rest_api(restApiId=api_id)
    return api["version"]


def lambda_handler(event, context):
    helper(event, context)
