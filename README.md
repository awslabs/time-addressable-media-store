# Time-addressable Media Store implementation

This solution deploys the AWS Infrastructure required to create sample implementation of the [BBC TAMS API](https://github.com/bbc/tams).

**NOTE: This solution is supplied as a reference TAMS API implementation. It is expected to be used in a development environment only to get familiar with the TAMS API. It is not supplied as a production-ready solution.**

## Pre-requisites

Before deploying this solution a CloudWatch log role ARN needs to be created and API Gateway configured to make use of it in your chosen region.

- [Create the required IAM role](https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-logging.html?icmpid=apigateway_console_help#set-up-access-logging-permissions)
- [Set up API Gateway to use this role](https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-logging.html?icmpid=apigateway_console_help#set-up-access-logging-using-console)

### Verify your VPC configuration

If you choose the option to deploy the solution in an existing VPC, you must ensure that the TAMS API Lambda functions running in the private subnets of your VPC can connect to other AWS services. This can be achieved with [NAT gateways](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-nat-gateway.html). However, this solution does not require internet access as long the required [VPC Endpoints](https://docs.aws.amazon.com/whitepapers/latest/aws-privatelink/what-are-vpc-endpoints.html) are available.

#### VPC Endpoints

If you choose to let this solution deploy a new VPC all the required VPC Endpoints will be created automatically.

If you are choosing to use an existing VPC then you should ensure that the subnets being supplied are able to access the internet or ensure you have the following VPC Endpoints in place:

| Service  | Endpoint Type |
| -------- | ------------- |
| s3       | Gateway       |
| dynamodb | Gateway       |
| sqs      | Interface     |
| events   | Interface     |
| ssm      | Interface     |

## Deploy the sample application

**NOTE: When deploying this solution please be sure to get the latest version from the [Releases](https://github.com/awslabs/time-addressable-media-store/releases) page in GitHub. The `main` branch often contains pre-release code that may not be fully stable.**

The Serverless Application Model Command Line Interface (SAM CLI) is an extension of the AWS CLI that adds functionality for building and testing Lambda applications. It uses Docker to run your functions in an Amazon Linux environment that matches Lambda. It can also emulate your application's build environment and API.

To use the SAM CLI, you need the following tools.

- SAM CLI - [Install the SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)
- Docker - [Install Docker community edition](https://hub.docker.com/search/?type=edition&offering=community)

To build and deploy your application for the first time, run the following in your shell:

```bash
sam build --use-container
sam deploy --guided
```

**NOTE: The Neptune database takes approximately 20mins to create. Neptune also requires a VPC which is why there are VPC parameters mentioned below.**

The first command will build the source of your application. The second command will package and deploy your application to AWS, with a series of prompts:

- **Stack Name**: The name of the stack to deploy to CloudFormation. This should be unique to your account and region, and a good starting point would be something matching your project name.
- **AWS Region**: The AWS region you want to deploy your app to.
- **VpcId**: Specify an existing VPC Id, leave blank to have one created.
- **VpcAZs**: Specify a comma-separated list of availability zones (for example `us-east-1a,us-east-1b`) to use when a VPC is specified. The number of AZs must match the number of private subnets specified in PrivateSubnetIds. Leave blank if VPC being created.
- **PrivateSubnetIds**: Specify a comma-delimited list of the Private Subnet Ids to be used in the existing VPC. **NOTE: These MUST be *PRIVATE* Subnets. When placed in a Public subnet Lambdas do not receive Public IPs and all network routing will fail. A Public subnet is determined by the default route being to an Internet Gateway.**
- **NeptuneDBInstanceClass**: Neptune Cluster Instance class, for example `db.serverless` or `db.r7g.large`
- **NeptuneServerlessConfiguration**: Neptune Serverless Scaling Configuration. Must be a list of two values, MinCapacity and MaxCapacity, separated by commas. Valid values between 1–128.
- **DeployWaf**: Specify whether you want the solution behind a WAF.
- **JwtIssuerUrl**: [Optional] The URL for the issuer of the JWT tokens you wish to authenticate with (e.g., your own identity provider). Leave this blank if you wish to deploy Cognito for auth or if providing your own Lambda Authorizer. **Note: Only one of JwtIssuerUrl or LambdaAuthorizerArn can be provided.**
- **LambdaAuthorizerArn**: [Optional] The ARN of an existing Lambda Authorizer to use for custom authentication logic. Leave blank to use the default Lambda Authorizer (which validates JWT tokens from either Cognito or your specified issuer). **Note: Only one of JwtIssuerUrl or LambdaAuthorizerArn can be provided.**
- **Confirm changes before deploy**: If set to yes, any change sets will be shown to you before execution for manual review. If set to no, the AWS SAM CLI will automatically deploy application changes.
- **Allow SAM CLI IAM role creation**: Many AWS SAM templates, including this example, create AWS IAM roles required for the AWS Lambda function(s) included to access AWS services. By default, these are scoped down to minimum required permissions. To deploy an AWS CloudFormation stack which creates or modifies IAM roles, the `CAPABILITY_IAM` value for `capabilities` must be provided. If permission isn't provided through this prompt, to deploy this example you must explicitly pass `--capabilities CAPABILITY_IAM` to the `sam deploy` command.
- **Save arguments to samconfig.toml**: If set to yes, your choices will be saved to a configuration file inside the project, so that in the future you can just re-run `sam deploy` without parameters to deploy changes to your application.

You can find your API Gateway Endpoint URL in the output values displayed after deployment.

## Usage

**Note:** An associated repository [time-addressable-media-store-tools](https://github.com/aws-samples/time-addressable-media-store-tools) is available to be used as a sample to give you a basic Web UI to help visualize your TAMS Store.

### Authorization

The API uses a Lambda Authorizer to validate JWT tokens and enforce scope-based authorization. You have three authentication options:

1. **Use the deployed Cognito User Pool** (default when both `JwtIssuerUrl` and `LambdaAuthorizerArn` are left blank)
   - Cognito User Pool is deployed
   - A Lambda Authorizer is deployed to validate Cognito-issued JWT tokens

2. **Use your own JWT issuer** (by providing `JwtIssuerUrl` during deployment)
   - Cognito is NOT deployed
   - A Lambda Authorizer is deployed to validate JWT tokens from your specified issuer
   - Useful when you have an existing identity provider that issues JWT tokens

3. **Use your own Lambda Authorizer** (by providing `LambdaAuthorizerArn` during deployment)
   - Cognito is NOT deployed
   - No Lambda Authorizer is deployed
   - You provide your own custom authorization logic

**Note:** Only one of `JwtIssuerUrl` or `LambdaAuthorizerArn` can be provided. If you provide either parameter, Cognito will not be created and Cognito-related outputs will be empty.

#### Using Cognito (Default)

The solution comes with an App Client already created for you. If required, you can create others for differing access patterns as required.

You will need to authenticate with the API using an `Authorization` header. The header follows the traditional `Bearer {token}` syntax.

The guide below assumes you are using **Client credentials** [OAuth 2.0 grant](https://docs.aws.amazon.com/cognito/latest/developerguide/federation-endpoints-oauth-grants.html) for "machine-to-machine" access using the pre-configured App Client.

To request an access token, you will require 4 pieces of information:

- **Client Id**: This can be retrieved from the Outputs of the Cloudformation Stack created during deployment or by browsing to the Cognito User Pool console and locating the App Client named `server-to-server`.

- **Client Secret**: This can be retrieved from the Cognito User Pool console and locating the App Client named `server-to-server`. Click on this App Client to view its details and you will see an option to copy the Client secret.
**NOTE: This Client secret is confidential, and care should be taken to store this value securely so that 3rd parties are not able to access it.**

- **Token URL**: This can be retrieved from the Outputs of the Cloudformation Stack created during deployment.

- **OAuth scope(s)**: Access to API methods is controlled by specific scopes. The options available are:
  - tams-api/admin - full administrative access to all methods
  - tams-api/read - required for GET and HEAD methods
  - tams-api/write - required for PUT and POST methods
  - tams-api/delete - required for DELETE methods

    **NOTE: Supplying more than one scope is allowed, it just means the resulting token will be able to call a combination of allowed methods.**

To request a token, you need to `POST` to the `TOKEN Url`:

```bash
curl -X POST -d 'client_id=XXX&client_secret=XXX&grant_type=client_credentials&scope=tams-api/read' https://XXXXX
```

This request will return JSON that will contain the `access_token`. Take the value of this token and create the `Authorization` header. Pass this header on all requests to the API to be authenticated.

Authorization is handled by the Lambda Authorizer which validates the JWT token and checks that the supplied OAuth scopes match the required scopes for the requested API method.

### Additional Storage

[Version 7](https://github.com/bbc/tams/releases/tag/7.0) of the TAMS API specification introduced new functionality that allows the client to choose between different "storage backends" where available.

This solution deploys with a default storage backend using a dedicated S3 bucket.

If desired, addtional S3 buckets can be deployed as separate TAMS storage backends. This is done by deploying separate Cloudformation Stacks with [this](storage_backend.yaml) template. It only takes a single parameter and that is the name of the main TAMS API stack.

> **Note:** This template does NOT require the SAM cli to deploy it. It can be done directly in the Cloudformation Console or via any other Cloudformation create stack option.

## Architecture Diagram

![Architecture Diagram](docs/images/architecture.png)

## Cleanup

To delete the solution, use the SAM CLI, you can run the following:

```bash
sam delete
```

> **After using `sam delete` the S3 Bucket and DynamoDB table(s) will still need to be deleted manually via the AWS Console if you do not want to retain the data.**

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
