import os
from functools import lru_cache

import boto3

idp = boto3.client("cognito-idp")

user_pool_id = os.environ["USER_POOL_ID"]


@lru_cache()
def get_user_pool():
    return idp.describe_user_pool(UserPoolId=user_pool_id)["UserPool"]


# pylint: disable=unused-argument
def lambda_handler(event, context):
    """Dervive a suitable username from the API Gateway request details"""
    if event.get("username"):
        user_pool = get_user_pool()
        if user_pool.get("UsernameAttributes"):
            user_attributes = idp.admin_get_user(
                UserPoolId=user_pool_id, Username=event["username"]
            )["UserAttributes"]
            user_attributes = {a["Name"]: a["Value"] for a in user_attributes}
            # Check attributes in order of preference
            for attr in ("email", "phone_number"):
                if (
                    user_pool["UsernameAttributes"].get(attr)
                    and attr in user_attributes
                ):
                    return user_attributes[attr]
        return event["username"]
    if event.get("client_id"):
        user_pool_client = idp.describe_user_pool_client(
            UserPoolId=user_pool_id, ClientId=event["client_id"]
        )
        return user_pool_client["UserPoolClient"]["ClientName"]
    return "NoAuth"
