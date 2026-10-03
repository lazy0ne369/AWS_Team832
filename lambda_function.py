import os
import json
import gzip
import time
import boto3
from urllib.parse import unquote_plus


# ---------------------------------------------------------
# AWS clients
# ---------------------------------------------------------

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")


# ---------------------------------------------------------
# Environment variables
# ---------------------------------------------------------

TABLE_NAME = os.environ["TABLE_NAME"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
SIZE_THRESHOLD_BYTES = int(
    os.environ.get("SIZE_THRESHOLD_BYTES", "1048576")
)

table = dynamodb.Table(TABLE_NAME)


# ---------------------------------------------------------
# VPC Flow Log fields
# ---------------------------------------------------------

FLOW_LOG_FIELDS = [
    "version",
    "accountId",
    "interfaceId",
    "srcAddr",
    "dstAddr",
    "srcPort",
    "dstPort",
    "protocol",
    "packets",
    "bytes",
    "start",
    "end",
    "action",
    "logStatus"
]


# ---------------------------------------------------------
# Convert numeric values safely
# ---------------------------------------------------------

def to_int(value):
    """
    Convert a Flow Log field to an integer.

    VPC Flow Logs can contain '-' for unavailable values.
    In that case, return None.
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------
# Send SNS alert
# ---------------------------------------------------------

def send_size_alert(bucket, key, size):
    message = (
        "VPC Flow Log size threshold exceeded.\n\n"
        f"Bucket: {bucket}\n"
        f"Object: {key}\n"
        f"Object size: {size} bytes\n"
        f"Threshold: {SIZE_THRESHOLD_BYTES} bytes\n\n"
        "Action: FlowLogProcessor detected a large VPC Flow Log object."
    )

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="VPC Flow Log Size Alert",
        Message=message
    )

    print(
        f"SNS alert sent: {key} "
        f"({size} bytes >= {SIZE_THRESHOLD_BYTES} bytes)"
    )


# ---------------------------------------------------------
# Convert one Flow Log line into a DynamoDB item
# ---------------------------------------------------------

def parse_flow_log_line(line):
    line = line.strip()

    if not line:
        return None

    parts = line.split()

    # We configured exactly 14 fields.
    if len(parts) != 14:
        print(
            f"Skipping malformed record. "
            f"Expected 14 fields, received {len(parts)}: {line}"
        )
        return None

    data = dict(zip(FLOW_LOG_FIELDS, parts))

    interface_id = data["interfaceId"]
    src_addr = data["srcAddr"]
    dst_addr = data["dstAddr"]
    dst_port_raw = data["dstPort"]
    start = data["start"]

    # -----------------------------------------------------
    # Sort key
    # -----------------------------------------------------

    event_key = (
        f"{start}#"
        f"{src_addr}#"
        f"{dst_addr}#"
        f"{dst_port_raw}"
    )

    # -----------------------------------------------------
    # Required DynamoDB attributes
    # -----------------------------------------------------

    item = {
        "interfaceId": interface_id,
        "eventKey": event_key,

        "version": to_int(data["version"]),
        "accountId": data["accountId"],

        "srcAddr": src_addr,
        "dstAddr": dst_addr,

        "action": data["action"],
        "logStatus": data["logStatus"]
    }

    # -----------------------------------------------------
    # Numeric attributes
    #
    # Only add them when the value is valid.
    # This prevents DynamoDB errors when AWS provides '-'.
    # -----------------------------------------------------

    numeric_fields = [
        "srcPort",
        "dstPort",
        "protocol",
        "packets",
        "bytes",
        "start",
        "end"
    ]

    for field in numeric_fields:
        value = to_int(data[field])

        if value is not None:

            # DynamoDB attribute names are camelCase.
            item[field] = value

    return item


# ---------------------------------------------------------
# Write items to DynamoDB in batches
# ---------------------------------------------------------

def write_batch(items):
    if not items:
        return

    # DynamoDB BatchWriteItem supports a maximum of 25 items.
    for i in range(0, len(items), 25):

        batch = items[i:i + 25]

        request_items = {
            TABLE_NAME: [
                {
                    "PutRequest": {
                        "Item": item
                    }
                }
                for item in batch
            ]
        }

        # Retry unprocessed items.
        while request_items.get(TABLE_NAME):

            response = dynamodb.meta.client.batch_write_item(
                RequestItems=request_items
            )

            unprocessed = response.get(
                "UnprocessedItems",
                {}
            )

            if not unprocessed.get(TABLE_NAME):
                break

            print(
                f"Retrying "
                f"{len(unprocessed[TABLE_NAME])} "
                f"unprocessed DynamoDB items..."
            )

            request_items = unprocessed

            time.sleep(0.5)


# ---------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------

def lambda_handler(event, context):

    print("Received S3 event:")
    print(json.dumps(event))

    total_records = 0
    written_records = 0
    skipped_records = 0

    # -----------------------------------------------------
    # Process every S3 record in the event
    # -----------------------------------------------------

    for record in event.get("Records", []):

        # Make sure this is an S3 event.
        if record.get("eventSource") != "aws:s3":
            continue

        bucket = record["s3"]["bucket"]["name"]

        key = unquote_plus(
            record["s3"]["object"]["key"]
        )

        object_size = int(
            record["s3"]["object"].get("size", 0)
        )

        print(
            f"Processing s3://{bucket}/{key} "
            f"({object_size} bytes)"
        )

        # -------------------------------------------------
        # Only process .log.gz files
        # -------------------------------------------------

        if not key.endswith(".log.gz"):
            print(f"Skipping non-log object: {key}")
            continue

        # -------------------------------------------------
        # Size threshold → SNS alert
        # -------------------------------------------------

        if object_size >= SIZE_THRESHOLD_BYTES:

            try:
                send_size_alert(
                    bucket,
                    key,
                    object_size
                )

            except Exception as error:
                print(
                    f"SNS alert failed: {error}"
                )

                # We continue processing the log even
                # if notification fails.
        
        # -------------------------------------------------
        # Get object from S3
        # -------------------------------------------------

        response = s3.get_object(
            Bucket=bucket,
            Key=key
        )

        body = response["Body"]

        # -------------------------------------------------
        # Stream gzip content
        #
        # This avoids loading the entire log into memory.
        # -------------------------------------------------

        with gzip.GzipFile(fileobj=body, mode="rb") as gz:

            batch_items = []

            for raw_line in gz:

                total_records += 1

                try:
                    line = raw_line.decode(
                        "utf-8"
                    ).strip()

                    item = parse_flow_log_line(line)

                    if item is None:
                        skipped_records += 1
                        continue

                    batch_items.append(item)

                    # Write in groups of 25.
                    if len(batch_items) == 25:

                        write_batch(batch_items)

                        written_records += len(
                            batch_items
                        )

                        batch_items = []

                except Exception as error:

                    skipped_records += 1

                    print(
                        f"Error processing line: {error}"
                    )

            # -------------------------------------------------
            # Write remaining records
            # -------------------------------------------------

            if batch_items:

                write_batch(batch_items)

                written_records += len(
                    batch_items
                )

    # -----------------------------------------------------
    # Final processing summary
    # -----------------------------------------------------

    result = {
        "status": "completed",
        "total_records": total_records,
        "written_records": written_records,
        "skipped_records": skipped_records
    }

    print(
        "Processing summary:"
    )

    print(
        json.dumps(result)
    )

    return {
        "statusCode": 200,
        "body": json.dumps(result)
    }