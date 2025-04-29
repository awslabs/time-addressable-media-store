from botocore.exceptions import ClientError
invoke = {
    "StatusCode": 300,
    "FunctionError": "Unhandled",
}

# if invoke["StatusCode"] != 200:
#     raise ClientError(
#         operation_name="LambdaInvoke", error_response=invoke["FunctionError"]
#     )

if invoke["StatusCode"] != 200:
    raise ClientError(
        operation_name="LambdaInvoke", error_response={'Error': {'Message': invoke["FunctionError"], 'Code': invoke['StatusCode']}}
    )
