# TAMS 4.1 to 5.0 Upgrade

**⚠️ IMPORTANT: This upgrade process is ONLY required when upgrading from version 4.1 to 5.0. Fresh installations of version 5.0 do NOT need this upgrade.**

## Overview

This CloudFormation template deploys a Lambda function that performs the necessary data transformations when upgrading from TAMS v4.1 to v5.0. The upgrade includes:

- **Webhooks ETL**: Migrates WebHook data from DynamoDB to Neptune.
- **Storage ETL**: Migrates FlowStorage data from the old table structure (with flow_id sort key) to the new structure (without sort key) and changes the `storage_ids` (list) field to `storage_id` (str) single value.
- **Tags ETL**: Updates tag properties in Neptune to serialise the existing strings. V5.0 stores all tags as serialised JSON strings so existing values need serialising.

## Prerequisites

**⚠️ DOWNTIME REQUIRED: The TAMS system will be in an unusable state after upgrading the CloudFormation stack to v5.0 until all ETL processes in this Lambda function are completed. Plan for scheduled downtime during this upgrade. The amount of downtime will vary depending on the volume of data held but is typically around 5 to 10 minutes.**

Before deploying this upgrade template, you must have:

1. Already upgraded your TAMS API CloudFormation stack from v4.1 to v5.0
2. The following outputs from your TAMS API v5.0 CloudFormation Stack:
   - LambdaSecurityGroup
   - LambdaSubnetIds
   - NeptuneClusterResourceId
   - NeptuneEndpoint
   - UtilsLayerArn
3. The names of your DynamoDB tables (the v5.0 upgrade retains the old tables for data migration):
   - Webhooks table name
   - Old FlowStorage table name (with flow_id sort key)
   - New FlowStorage table name (without sort key)

## Deployment

Deploy the CloudFormation template using the AWS CloudFormation Console:

1. Open the [AWS CloudFormation Console](https://console.aws.amazon.com/cloudformation/)
2. Choose **Create stack** > **With new resources (standard)**
3. For **Specify template**, choose **Upload a template file**
4. Choose **Choose file** and enter `tams-4.1-5.0-upgrade-template.yaml`
5. Choose **Next**
6. Enter a **Stack name** (for example, `tams-upgrade-4-1-to-5-0`)
7. Fill in the parameters with values from your TAMS API v5.0 stack outputs and DynamoDB table names
8. Choose **Next** to proceed to the Configure stack options page
9. Select all the options in the Capabilities and transforms box
10. Choose **Next** to proceed to the Review and create
11. Choose **Submit**

## Running the Upgrade

The Lambda function contains three separate ETL processes. Each must be run individually by uncommenting the appropriate line in the Lambda code.

### Step 1: Webhooks ETL

1. Open the Lambda function in the AWS Console (named `<stack-name>-LambdaFunction-<id>`)
2. Execute the Lambda function (choose "Test" with an event of `{"etl_step": "webhooks"}`)
3. Verify completion in the Lambda Execution Results window. (`SUCCESS` appears in the output)

**Note: The WebHooks ETL process can only be executed once. If run a second time it will create duplicate WebHooks in the TAMS Store.**

### Step 2: Storage ETL

1. Execute the Lambda function (choose "Test" with an event of `{"etl_step": "storage"}`)
2. Verify completion in the Lambda Execution Results window. If all items were not processed `PARTIAL` will be visible in the output.
3. If a `last_evaluated_key` was logged, update the Lambda Test Event to include it: `{"etl_step": "storage", "last_evaluated_key": <value>}` and re-run
4. Repeat until all items are processed. (`SUCCESS` appears in the output)

**Note: Since the ids of the items are preserved in this ETL process, re-running the process will not cause any harm.**

### Step 3: Tags ETL

1. Execute the Lambda function (choose "Test" with an event of `{"etl_step": "tags"}`)
2. Verify completion in the Lambda Execution Results window. If all items were not processed `PARTIAL` will be visible in the output.
3. If processing is incomplete, re-run the function (it will continue processing remaining items)
4. Repeat until all items are processed. (`SUCCESS` appears in the output)

**Note: Since this ETL process updates the items in place and the query filters to only items than need processing re-running the process will not cause any harm.**

## Verification

Before cleanup, verify all ETL processes completed successfully by testing the TAMS API:

1. **Verify Webhooks ETL**: List webhooks from the TAMS API and confirm they are returned correctly
2. **Verify Storage ETL**: Call the Objects endpoint and confirm the `first_referenced_by_flow` field is displayed
3. **Verify Tags ETL**: Call the TAMS API to retrieve a Flow and confirm tags are displayed correctly

If all three checks pass, the ETL processes completed successfully, and you can proceed with cleanup.

## Cleanup

After successfully completing all three ETL processes and verification:

1. Delete the upgrade stack:
   - Open the [AWS CloudFormation Console](https://console.aws.amazon.com/cloudformation/)
   - Select the upgrade stack (for example, `tams-upgrade-4-1-to-5-0`)
   - Choose **Delete**
   - Confirm the deletion

2. Delete the old DynamoDB tables:
   - Manually delete the old Webhooks DynamoDB table
   - Manually delete the old FlowStorage DynamoDB table

## Notes

- Each ETL process is designed to run independently to avoid timeout issues
- The Lambda function has a 15-minute timeout and 10GB memory allocation
- Storage and Tags ETL processes include timeout handling and can be re-run
- Depending on the volume of data in your TAMS store, some ETL processes may need to be executed multiple times to migrate all data
- Monitor Lambda Execution Results for progress and any error messages
