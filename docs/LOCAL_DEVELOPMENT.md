# Local Development

This guide provides instructions for setting up and working with the Time-addressable Media Store project in a local development environment.

## Pre-Requisites

The following dependencies must be installed. Please refer to your operating system documentation for installation instructions.

> **Note:** For Windows, we recommend enabling Windows Subsystem for Linux (WSL) and installing a Linux distribution of your choice,
> for example, here are the instructions on how to install [Ubuntu](https://ubuntu.com/tutorials/ubuntu-on-windows).

- Python ~= 3.13.0 and pip
- [poetry](https://python-poetry.org/) - Python dependency management
- [pre-commit](https://pre-commit.com/#install) - Git hooks manager
- [cfn-lint](https://github.com/aws-cloudformation/cfn-lint) - CloudFormation linting
- [cfn-nag](https://github.com/stelligent/cfn_nag) - CloudFormation security scanning
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html) - Serverless Application Model CLI
- [Docker](https://hub.docker.com/search/?type=edition&offering=community) - Required for local Lambda function testing
- [rain](https://github.com/aws-cloudformation/rain) - CloudFormation template formatting
- [git-secrets](https://github.com/awslabs/git-secrets) - Prevents committing secrets

## Setting Up Your Development Environment

### 1. Clone the Repository

```bash
git clone https://github.com/awslabs/time-addressable-media-store.git
cd time-addressable-media-store
```

### 2. Install Python Dependencies

```bash
poetry install
```

### 3. Set Up Pre-commit Hooks

```bash
pre-commit install
```

## Development Workflow

### Running Tests

The project includes unit, functional, and acceptance tests:

```bash
# Run unit tests
make test-unit

# Run functional tests
make test-functional

# Run acceptance tests
make test-acceptance

# Run all tests
make test
```

### Code Formatting and Linting

```bash
# Format code with black and isort
make format

# Run linting checks
make lint

# Run CloudFormation linting
make cfn-lint

# Run CloudFormation security checks
make cfn-nag

# Format CloudFormation templates
make cfn-format
```

### API Development

To generate the API schema from the TAMS repository:

```bash
make api-spec
```

You can specify a specific tag:

```bash
make api-spec TAMS_TAG=v1.0.0
```

### Local Testing

#### Running API Gateway Locally

```bash
make local-api
```

#### Invoking Lambda Functions Locally

```bash
make local-invoke FUNCTION_NAME=<function-name> EVENT_FILE=<path-to-event-json>
```

### Building and Deploying

#### Build the SAM Application

```bash
make build
```

#### Validate the SAM Template

```bash
make validate
```

#### Deploy the SAM Application

```bash
make deploy STACK_NAME=<stack-name> AWS_REGION=<region> PROFILE=<aws-profile>
```

Default values:
- STACK_NAME: tams
- AWS_REGION: us-east-1
- PROFILE: default

## Project Structure

- `/api` - API specification and build files
- `/docs` - Documentation and architecture diagrams
- `/functions` - Lambda function code organized by service
- `/layers` - Shared Lambda layers
- `/templates` - CloudFormation templates for infrastructure components
- `/tests` - Test files organized by test type (unit, functional, acceptance)
- `/upgrades` - Upgrade templates for version migrations

## Cleaning Up

To clean build artifacts:

```bash
make clean
```

## Additional Resources

- See the main [README.md](../README.md) for deployment instructions and architecture overview
- For contributing guidelines, see [CONTRIBUTING.md](../CONTRIBUTING.md)
- For license information, see [LICENSE](../LICENSE)
