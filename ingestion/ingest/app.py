import os
import logging
import requests
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
from glom import glom, PathAccessError
from decimal import Decimal

# setup logging
log = logging.getLogger()
log.setLevel(logging.INFO)

TABLE_NAME = os.environ.get("TABLE_NAME", "escape-cville-table")
REGION = os.environ.get("REGION", "us-east-1")

# Get API key from SSM
def get_api_key():
    ssm = boto3.client("ssm", region_name="us-east-1")
    try:
        response = ssm.get_parameter(
            Name="/escape-cville/here-api-key",
            WithDecryption=True
        )
    except Exception as e:
        log.error("Failed to fetch api key from SSM: %s", e)
        quit()
    return response["Parameter"]["Value"]
HERE_API_KEY = get_api_key()


def handler(event, context):
    """Called by CloudWatch scheduler, every 15 or 30 minutes."""

    # ingest north and south
    check_time = datetime.now(timezone.utc).isoformat()
    r1 = ingest("north", check_time)
    r2 = ingest("south", check_time)

    max_status_code = max(r1["statusCode"], r2["statusCode"])
    return {"statusCode": max_status_code, "body": f"{r1['body']}\n{r2['body']}"}


def ingest(direction, check_time):
    """Runs ingestion for a direction."""
    log.info(f"Starting ingestion run: {direction}")

    # Fetch data
    try:
        data = fetch_data(direction)
    except Exception as e:
        log.error("Failed to fetch data: %s", e)
        return {"statusCode": 500, "body": str(e)}
    
    # Get previous item
    try:
        previous_item = get_previous(direction)
    except ClientError as e:
        log.error("Failed to get previous item: %s", e)
        return {"statusCode": 500, "body": str(e)}
    except PathAccessError as e:
        log.error("Failed to get previous item: %s", e)
        return {"statusCode": 500, "body": str(e)}
    
    # Calculate delta
    if previous_item is None:
        delta = 0
    else:
        delta = data['duration'] - Decimal(str(previous_item['duration']))

    # Format item
    item = {
        "route": direction, # partition key
        "timestamp": check_time, # sort key
        "duration": data['duration'], 
        "base_duration": data['base_duration'],
        "distance": data['distance'],
        "delta": delta
    }

    try:
        write_to_dynamo(item)
    except Exception as e:
        log.error("Failed to write to DynamoDB: %s", e)
        return {"statusCode": 500, "body": str(e)}

    log.info("Wrote item: %s", item)
    return {"statusCode": 200, "body": "ok"}


def fetch_data(direction):
    """Fetches from HERE API; Return all data."""
    if HERE_API_KEY is None:
        raise ValueError("HERE_API_KEY is not set")

    if direction == "south": # SDS to 29/64
        origin = "38.0405308,-78.5079088"
        destination = "38.0223135,-78.5341943"
    elif direction == "north": # 29/64 to SDS
        origin = "38.0195483,-78.5387543"
        destination = "38.0405308,-78.5079088"
    else:
        raise ValueError("Unknown direction: %s" % direction)

    url = "https://router.hereapi.com/v8/routes"
    params = {
        "transportMode": "car",
        "origin": origin,
        "destination": destination,
        "routingMode": "fast",
        "return": "summary",
        "apikey": HERE_API_KEY
    }
    response = requests.get(url, params=params)

    response.raise_for_status()
    data = response.json()

    # see if needed key/values exist
    duration = glom(data, "routes.0.sections.0.summary.duration")
    base_duration = glom(data, "routes.0.sections.0.summary.baseDuration")
    distance = glom(data, "routes.0.sections.0.summary.length")

    return {
        "duration": duration,
        "base_duration": base_duration,
        "distance": distance
    }


def get_previous(direction):
    """Return the latest stored item for the given direction."""
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(TABLE_NAME)

    resp = table.query(
        KeyConditionExpression=Key("route").eq(direction),
        ScanIndexForward=False,   # descending timestamp order
        Limit=1,
    )

    items = resp.get("Items", [])
    return items[0] if items else None


def write_to_dynamo(item):
    """Write the item to DynamoDB."""
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(Item=item)


if __name__ == "__main__":
    pass
    # handler(None, None)