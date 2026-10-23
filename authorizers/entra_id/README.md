# Dedicated Microsoft Entra ID Lambda Authorizer for TAMS API (Option B)

This package implements **Option B**: a standalone, decoupled Lambda Authorizer for Microsoft Entra ID (Azure Active Directory) tailored for the Time-addressable Media Store (TAMS) API.

---

## Why Option B?

- **Upstream Maintainability:** Modifying core upstream files (like `template.yaml` or `functions/lambda_authorizer/app.py`) creates ongoing merge conflicts when pulling upstream updates from `awslabs/time-addressable-media-store`.
- **Decoupled Lifecycle:** The authorizer can be deployed, upgraded, monitored, and scaled independently of the main TAMS API stack.
- **Native Integration:** Uses the pre-existing, built-in `LambdaAuthorizerArn` CloudFormation parameter in the main TAMS `template.yaml`.

---

## Token Support

The authorizer supports both Microsoft Entra ID token models:

### 1. Delegated Tokens (User + Application)
- **Grant Types:** Authorization Code Flow, On-Behalf-Of (OBO) flow, Interactive User logins.
- **Scopes:** Extracted from the `scp` (or `scope`) claim (e.g. `tams-api.read tams-api.write`).
- **User Identity:** Extracted in priority order for TAMS `created_by` / `updated_by` auditing:
  1. `preferred_username` (e.g. `jane.doe@example.com`)
  2. `upn` (User Principal Name, e.g. `jane.doe@company.onmicrosoft.com`)
  3. `email`
  4. `unique_name`
  5. `name`
  6. `sub` / `oid` (fallback)
- **Auth Classes:** Mapped from `groups`, `roles`, or custom `auth_classes` claims.

### 2. Application-Only Tokens (Machine-to-Machine / Daemon)
- **Grant Types:** Client Credentials Flow (`client_id` + `client_secret` or certificate).
- **Scopes/Permissions:** In Entra ID app-only tokens, `scp` is absent. Application permissions are delivered in the **`roles`** array claim (e.g. `["tams-api.read.app", "tams-api.write.app"]` or `["tams-api.admin.app"]`). The authorizer normalizes and maps these roles directly to route authorization checks.
- **Caller Identity:** Extracted from:
  1. `app_displayname` (e.g. `TAMS-Ingest-Worker`)
  2. `azp` / `appid` (Client ID GUID)
  3. `oid` / `sub` (Service Principal Object ID)
- **Principal ID:** Service Principal `oid` / `sub`.

### Scope Normalization
The authorizer automatically normalizes:
- **Role Suffixes:** Automatically strips `.app` and `.all` suffixes (e.g. `tams-api.read.app` -> `tams-api.read`) so App Roles and Delegated scopes resolve to the same route authorization rules in `oauth_scopes.json`.
- **Delimiter Formats:** Accepts both dot (`tams-api.read`) and slash (`tams-api/read`) formats, ensuring backward compatibility with both `oauth_scopes.json` and backend Lambda authorizer decorators.

---

## Entra ID App Registration Setup

1. In Microsoft Entra ID, register your **TAMS API Application** (Resource Server).
2. **Expose an API (Delegated Permissions):**
   - Application ID URI: `api://<client-id>` (or `api://tams-api`)
   - Add scopes:
     - `tams-api.read` (Display: Read TAMS resources)
     - `tams-api.write` (Display: Write TAMS resources)
     - `tams-api.delete` (Display: Delete TAMS resources)
     - `tams-api.admin` (Display: Administrator access)
3. **App Roles (Application / App-Only Permissions):**
   - Create App Roles with Allowed member types: **Applications** (using the `.app` suffix to prevent manifest collision with exposed scopes):
     - `tams-api.read.app` (Value: `tams-api.read.app`)
     - `tams-api.write.app` (Value: `tams-api.write.app`)
     - `tams-api.delete.app` (Value: `tams-api.delete.app`)
     - `tams-api.admin.app` (Value: `tams-api.admin.app`)
4. **Calling Applications:**
   - For backend services (Client Credentials): Grant the above App Roles under **API Permissions** -> **Application permissions** and grant Admin Consent.
   - For user applications: Grant Delegated permissions under **API Permissions** -> **Delegated permissions**.

---

## Deployment Strategy (Dev and Prod)

Deploy the unified CloudFormation stack (`main.yaml`) which automatically provisions both the Entra ID Authorizer stack and the main TAMS API stack, passing the authorizer ARN automatically.

### Unified Deployment

Using `samconfig.toml` (recommended):

#### Development
```bash
sam build --config-env dev
sam deploy --config-env dev
```

#### Production
```bash
sam build --config-env prod
sam deploy --config-env prod
```

The authorizer ARN is automatically passed from the nested authorizer stack to the main TAMS stack within `main.yaml`.

---

### Standalone Authorizer Deployment (Optional / Legacy)

If deploying the authorizer as a standalone stack independently:

```bash
sam build --template-file entra_authorizer.yaml
sam deploy \
  --stack-name tams-entra-authorizer-dev \
  --resolve-s3 \
  --region eu-west-1 \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    TenantId="be189d14-1d13-4148-9edf-47fff7e918a8" \
    AllowedAudiences="api://8ab97629-3017-4d9e-907d-a7201a4ffd25" \
    LogLevel="DEBUG"
```

---

## Verification & Testing

Run the dedicated authorizer unit test suite:
```bash
pytest tests/unit/authorizers/test_entra_authorizer.py -v
```
