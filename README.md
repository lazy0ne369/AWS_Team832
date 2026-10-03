# Amazon VPC Flow Logs Analysis for Traffic Auditing

A cost-conscious, serverless AWS project for collecting Amazon VPC Flow Logs, processing them, indexing them in DynamoDB, querying them through an API, visualizing results in a lightweight dashboard, and sending SNS alerts when a log object crosses a configured size threshold.

## Architecture

```text
EC2 traffic
    |
    v
VPC Flow Logs
    |
    v
S3: vpc-flow-audit-team-382
    |
    | ObjectCreated (.log.gz)
    v
Lambda: FlowLogProcessor
    |---------------------------> SNS -> Email
    |
    v
DynamoDB: VPCFlowLogs
    |
    +--> ActionIndex
    +--> SourceIpIndex
    +--> DestinationPortIndex
    |
    v
Lambda: VPCFlowQuery
    |
    v
API Gateway: VPCFlowAuditAPI
    |
    +--> GET /logs
    |
    v
Lambda: VPCFlowDashboard
    |
    v
Dashboard
```

## AWS services

- Amazon VPC / VPC Flow Logs
- Amazon S3
- AWS Lambda
- Amazon DynamoDB
- Amazon SNS
- Amazon API Gateway
- Amazon EC2 (traffic generation/testing)
- AWS IAM
- Amazon CloudWatch Logs

## DynamoDB design

Table: `VPCFlowLogs`

Primary key:
- Partition key: `interfaceId` (String)
- Sort key: `eventKey` (String)

GSIs:
- `ActionIndex`: `action` / `eventKey`
- `SourceIpIndex`: `srcAddr` / `eventKey`
- `DestinationPortIndex`: `dstPort` / `eventKey`

The application uses DynamoDB `Query` operations rather than table scans for the supported filters.

## API

`GET /logs`

Supported filters:
- `action`
- `sourceIp`
- `destinationPort`

Optional:
- `limit` from 1 to 100
- `nextToken` for pagination

Example:

```text
/logs?action=REJECT&limit=10
/logs?sourceIp=<source-ip>&limit=10
/logs?destinationPort=443&limit=10
```

The deployed API URL is intentionally not hard-coded in source files. Configure it in project documentation/deployment notes if you want to publish it.

## Lambda environment variables

### FlowLogProcessor

```text
TABLE_NAME=VPCFlowLogs
SNS_TOPIC_ARN=<SNS topic ARN>
SIZE_THRESHOLD_BYTES=1048576
```

### VPCFlowQuery

```text
TABLE_NAME=VPCFlowLogs
```

### VPCFlowDashboard

No application environment variables are required.

## IAM principle

The project uses separate execution roles.

`FlowLogProcessor`:
- S3 GetObject for the flow-log objects
- DynamoDB PutItem / BatchWriteItem for the flow-log table
- SNS Publish to the alert topic
- CloudWatch Logs permissions

`VPCFlowQuery`:
- DynamoDB Query only
- Access limited to the table and its three GSIs

`VPCFlowDashboard`:
- Basic Lambda execution/logging permissions only

No AWS credentials or access keys belong in this repository.

## Cost-conscious decisions

The project intentionally avoids:
- NAT Gateway
- Athena
- QuickSight
- OpenSearch
- RDS
- Redshift
- provisioned Lambda concurrency
- customer-managed KMS keys
- unnecessary DynamoDB capacity increases

DynamoDB uses provisioned capacity with low capacity values suitable for a small project/demo workload. AWS free-tier eligibility varies by account and region, so actual charges should still be monitored.

## Security

- S3 Block Public Access enabled
- S3 server-side encryption
- DynamoDB encryption at rest
- Least-privilege Lambda roles
- No public S3 objects
- No credentials committed to GitHub
- Dashboard and API are exposed through API Gateway; DynamoDB is not directly exposed

## Deployment

1. Create the VPC and EC2 test environment.
2. Enable VPC Flow Logs to S3 using the required custom 14-field format.
3. Create `VPCFlowLogs` and its GSIs.
4. Create the SNS topic and confirm the email subscription.
5. Deploy `FlowLogProcessor` and configure the S3 ObjectCreated trigger.
6. Deploy `VPCFlowQuery`.
7. Create HTTP API Gateway route `GET /logs`.
8. Deploy `VPCFlowDashboard`.
9. Create HTTP API Gateway route `GET /dashboard`.
10. Generate small amounts of test traffic and verify S3, Lambda, DynamoDB, SNS, API and dashboard behavior.

See `docs/deployment-notes.md` for project-specific configuration notes.

## Repository safety

Before making this repository public, search for:
- AWS access keys
- secret keys
- passwords
- private tokens
- personal email addresses you do not want public
- private/internal hostnames

Do not commit raw production logs unless they have been intentionally sanitized.
