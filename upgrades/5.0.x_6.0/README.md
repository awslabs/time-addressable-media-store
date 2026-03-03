# TAMS 5.0.x to 6.0 Upgrade

## Overview

Version 6.0.0 introduces significant architectural changes to support flexible authentication options, including optional Cognito, external OIDC providers, and custom Lambda Authorizers. These changes include **breaking changes** that require careful planning before upgrading.

## ⚠️ BREAKING CHANGES

### 1. Cognito Resources Relocated to Nested Stack

**Impact:** All existing Cognito resources will be **DELETED and RECREATED** during the upgrade.

**What's happening:**

- In v5.0.x, Cognito resources (UserPool, UserPoolClient, etc.) were defined directly in the main CloudFormation template
- In v6.0.0, these resources have been moved to a separate nested stack ([templates/cognito.yaml](../../
templates/cognito.yaml))
- CloudFormation treats this as deletion of old resources and creation of new resources
- **All existing Cognito users, user pool clients, and settings will be LOST**

**Required Actions:**

1. **Before upgrading:**
   - Document all existing Cognito users and their permissions
   - Export any custom Cognito configuration settings
   - Document all user pool clients (beyond the default ones)
   - Save any custom OAuth scopes or resource server configurations

2. **After upgrading:**
   - Recreate users in the new User Pool
   - Reconfigure any custom settings
   - Update client applications with new User Pool ID and Client IDs
   - Users will need to be recreated by administrators

### 2. Removed Stack Outputs and Exports

The following stack outputs have been **completely removed** and will cause failures in any dependent stacks:

- **`UserPoolClientWebId`** - The web client for front-end user authentication has been removed. This functionality has been moved to the [time-addressable-media-store-tools](https://github.com/aws-samples/time-addressable-media-store-tools) repository.

- **`IdentityPoolId`** - The Cognito Identity Pool for federated identities has been removed. This functionality has been moved to the [time-addressable-media-store-tools](https://github.com/aws-samples/time-addressable-media-store-tools) repository.

- **`AuthRoleName`** - The IAM role for authenticated users has been removed along with the Identity Pool.

**Required Actions:**

1. **Before upgrading:**
   - Identify all CloudFormation stacks that import these values using:

     ```bash
     aws cloudformation list-imports --export-name <stack-name>-UserPoolClientWebId
     aws cloudformation list-imports --export-name <stack-name>-IdentityPoolId
     aws cloudformation list-imports --export-name <stack-name>-AuthRoleName
     ```

   - Update or remove dependent stacks that reference these exports

### 3. Conditional Stack Outputs and TAMS Tools Compatibility

The following stack outputs are now **conditional** and will return `'-'` (dash) if Cognito is not deployed:

- **`UserPoolId`**
- **`TokenUrl`**
- **`UserPoolClientId`**

**Impact on TAMS Tools:**

If you have deployed the [time-addressable-media-store-tools](https://github.com/aws-samples/time-addressable-media-store-tools) stack, **this upgrade will FAIL** because TAMS Tools imports one of the stack outputs that will change during the upgrade.

Additionally, the TAMS Tools stack will be broken by this upgrade due to:

1. Changes to the authentication mechanism in TAMS 6.0.0
2. Cognito resources being recreated with new IDs
3. Changes to stack outputs

**Required Actions:**

The recommended (and required) upgrade path is:

1. **Delete the TAMS Tools stack** (if deployed) - The upgrade will fail if you skip this step
2. **Upgrade this TAMS stack to v6.0.0**
3. **Re-deploy the TAMS Tools stack** using the updated version that is compatible with TAMS 6.0.0

**Note:** The TAMS Tools stack will need to be upgraded to a version compatible with TAMS 6.0.0 anyway, so deletion and redeployment is the cleanest approach

### 4. VPC Endpoint Changes

The **Lambda VPC endpoint** requirement has been removed from the deployment prerequisites.

**Impact:** If you were using an existing VPC, you no longer need to have a Lambda VPC endpoint configured.

## 🎉 New Features

### 1. Flexible Authentication Options

Version 6.0.0 introduces flexible authentication with three options:

- **Use Deployed Cognito** (default) - Automatic Cognito deployment with Lambda Authorizer
- **Use Your Own JWT Issuer** (OIDC) - Bring your own identity provider (Auth0, Okta, Azure AD, etc.)
- **Use Your Own Lambda Authorizer** - Complete custom authorization logic

**See the [Authorization section in the main README](../../README.md#authorization) for complete details on:**

- How to configure each authentication option
- Required JWT token claims for custom issuers
- Lambda Authorizer context requirements
- OAuth scopes and how to use them

## 🔄 Upgrade Process

### Pre-Upgrade Checklist

- [ ] Review all breaking changes above
- [ ] Identify dependent stacks using removed exports
- [ ] Document all Cognito users and settings
- [ ] Decide on authentication strategy (Cognito, OIDC, or custom)
- [ ] Plan for user recreation and notification
- [ ] Schedule maintenance window for upgrade
- [ ] Backup any critical configuration or data

### Recommended Upgrade Steps

1. **Audit Dependencies:**

   ```bash
   # Check for stacks importing removed outputs
   aws cloudformation list-imports --export-name <stack-name>-UserPoolClientWebId
   aws cloudformation list-imports --export-name <stack-name>-IdentityPoolId
   aws cloudformation list-imports --export-name <stack-name>-AuthRoleName
   aws cloudformation list-imports --export-name <stack-name>-UserPoolId
   ```

2. **Update or Remove Dependent Stacks:**
   - Update dependent stacks to remove references to deleted exports
   - Or delete dependent stacks if they're no longer needed
   - Deploy these changes BEFORE upgrading TAMS

3. **Document Cognito Configuration:**

   ```bash
   # Get current User Pool configuration
   aws cognito-idp describe-user-pool --user-pool-id <user-pool-id>

   # List all users (repeat with pagination token if needed)
   aws cognito-idp list-users --user-pool-id <user-pool-id>

   # List all user pool clients
   aws cognito-idp list-user-pool-clients --user-pool-id <user-pool-id>
   ```

4. **Deploy v6.0.0:**

   ```bash
   sam build
   sam deploy --guided
   ```

   During deployment, you'll be prompted for the new parameters:
   - Leave `JwtIssuerUrl` blank to use Cognito (default)
   - Leave `LambdaAuthorizerArn` blank to use the default Lambda Authorizer
   - Or provide values if using external authentication

5. **Recreate Cognito Users (if using Cognito):**

   ```bash
   # Create admin user
   aws cognito-idp admin-create-user \
     --user-pool-id <new-user-pool-id> \
     --username admin@example.com \
     --user-attributes Name=email,Value=admin@example.com Name=email_verified,Value=true
   ```

6. **Update Client Applications:**
   - Update applications with new User Pool ID and Client ID (from stack outputs)
   - Update API endpoint if changed
   - Test authentication flows

7. **Verify Functionality:**
   - Test API access with new authentication
   - Verify scope-based authorization works correctly
   - Test all dependent stacks

## 📚 Additional Resources

- [Main README](../../README.md) - Updated with new authentication documentation
- [TAMS Tools Repository](https://github.com/aws-samples/time-addressable-media-store-tools) - For front-end authentication components

## ❓ Getting Help

If you encounter issues during the upgrade:

1. Check the [GitHub Issues](https://github.com/aws-samples/time-addressable-media-store/issues)
2. Review CloudFormation stack events for specific error messages
3. Verify all dependent stacks have been updated
4. Open a new issue with details about your upgrade scenario
