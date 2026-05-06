import os
import io
import logging
import requests
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
from glom import glom, PathAccessError
from decimal import Decimal
from zoneinfo import ZoneInfo
import numpy as np

# Setup logging
log = logging.getLogger()
log.setLevel(logging.INFO)

TABLE_NAME = os.environ.get("TABLE_NAME", "escape-cville-table")
REGION = os.environ.get("REGION", "us-east-1")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "escape-cville-bucket")

dynamodb = boto3.resource("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

# Get API key from SSM
HERE_API_KEY = None
def get_api_key():
    global HERE_API_KEY
    if HERE_API_KEY:
        return HERE_API_KEY

    try:
        response = ssm.get_parameter(
            Name="/escape-cville/here-api-key",
            WithDecryption=True
        )
        HERE_API_KEY = response["Parameter"]["Value"]
        return HERE_API_KEY
    except ClientError as e:
        log.exception("SSM get_parameter failed")
        raise


# Handler
def handler(event, context) -> dict:
    """Called by CloudWatch scheduler, every 20 minutes. Returns status code and body."""

    # ingest north and south
    check_time = datetime.now(timezone.utc).isoformat()
    r1, north_item = ingest("north", check_time)
    r2, south_item = ingest("south", check_time)

    # update metadata
    if north_item:
        r3 = update_metadata("north", north_item)
    else:
        r3 = {"statusCode": 500, "body": "north ingestion failed"}
    if south_item:
        r4 = update_metadata("south", south_item)
    else:
        r4 = {"statusCode": 500, "body": "south ingestion failed"}

    # update plots
    update_plots()

    max_status_code = max(r1["statusCode"], r2["statusCode"], r3["statusCode"], r4["statusCode"])
    return {"statusCode": max_status_code, "body": f"{r1['body']}\n{r2['body']}\n{r3['body']}\n{r4['body']}"}


# Ingestion Helpers
def ingest(direction, check_time)  -> dict:
    """Runs ingestion for a direction. Time is preset. Returns status code and body dict and item dict if successful."""
    log.info("Starting ingestion run direction=%s", direction)

    # Fetch data
    try:
        data = fetch_data(direction)
    except Exception as e:
        log.exception("Failed to fetch data")
        return {"statusCode": 500, "body": f"{direction} ingestion failed: {e}"}, None
    
    # Get previous item
    try:
        previous_item = get_previous(direction)
    except ClientError as e:
        log.error("Client Error:Failed to get previous item: %s", e)
        return {"statusCode": 500, "body": f"{direction} ingestion failed: {e}"}, None
    except Exception as e:
        log.error("Failed to get previous item: %s", e)
        return {"statusCode": 500, "body": f"{direction} ingestion failed: {e}"}, None
    
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
    except ClientError as e:
        log.exception("DynamoDB write failed")
        return {"statusCode": 500, "body": f"{direction} ingestion failed: {e}"}, None

    log.info("Wrote item: %s", item)
    return {"statusCode": 200, "body": f"Successfully wrote item: {item}"}, item

def fetch_data(direction) -> dict:
    """Fetches from HERE API; Returns duration, base_duration, distance."""
    api_key = HERE_API_KEY or get_api_key() # raises if fails

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
        "apikey": api_key
    }

    response = None
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
    except requests.HTTPError:
        log.error(
            "HERE API HTTP error: status=%s body=%s",
            response.status_code if response else "N/A",
            response.text if response else "N/A"
        )
        raise
    except requests.RequestException:
        log.exception("HERE API request failed")
        raise

    try:
        data = response.json()
    except ValueError:
        log.error("Failed to parse HERE API response as JSON: %s", response.text)
        raise

    # see if needed key/values exist
    try:
        duration = glom(data, "routes.0.sections.0.summary.duration")
        base_duration = glom(data, "routes.0.sections.0.summary.baseDuration")
        distance = glom(data, "routes.0.sections.0.summary.length")
    except PathAccessError:
        log.error("HERE API response missing expected fields: %s", data)
        raise

    return {
        "duration": duration,
        "base_duration": base_duration,
        "distance": distance
    }

def get_previous(direction) -> dict:
    """Return the latest stored item for the given direction from DynamoDB."""
    resp = table.query(
        KeyConditionExpression=Key("route").eq(direction),
        ScanIndexForward=False,   # descending timestamp order
        Limit=1,
    )

    items = resp.get("Items", [])
    return items[0] if items else None

def write_to_dynamo(item):
    """Write the item to DynamoDB."""
    table.put_item(Item=item)


# MetaData Helpers
def update_metadata(direction, item) -> dict:
    """Runner for updating metadata. Returns status code and body."""
    log.info("Starting metadata update run direction=%s", direction)

    try:
        # check if file exists in s3
        meta_df = get_s3_csv_file(f"data/{direction}_metadata.csv") # returns None if file doesn't exist

        # if not, do cold start
        if meta_df is None:
            new_meta_df = cold_start_meta(direction)

        # otherwise, update single item
        else:
            new_meta_df = meta_update_item(meta_df, item)

    except Exception:
        log.exception("Metadata DF update failed for direction=%s", direction)
        return {"statusCode": 500, "body": "metadata DF update failed"}

    # save to s3
    try:
        save_s3_csv_file(new_meta_df, f"data/{direction}_metadata.csv")
    except Exception:
        log.exception("Metadata update failed save to s3 for direction=%s", direction)
        return {"statusCode": 500, "body": "metadata update failed save to s3"}

    log.info("Finished metadata update run direction=%s", direction)
    return {"statusCode": 200, "body": f"Successfully updated metadata for {direction}"}

def meta_update_item(meta_df, item) -> pd.DataFrame:
    """Update metadata for a single item using Welford's algorithm. Returns updated df."""

    # get hour and is_weekend in east coast time zone (round to nearest hour)
    dt_eastcoast, dt_ec_rounded, hour, is_weekend = get_datetime_info(item["timestamp"])

    # get weekday/weekend suffix and index for hour
    suffix = "we" if is_weekend else "wd"
    matches = meta_df.index[meta_df["hour"] == hour]
    if len(matches) == 0:
        raise ValueError(f"No row found for hour={hour}")
    idx = matches[0]

    # get current mean, count, m2; get new value
    n = meta_df.at[idx, f"{suffix}-count"]
    mean = meta_df.at[idx, f"{suffix}-mean"]
    m2 = meta_df.at[idx, f"{suffix}-m2"]
    x = float(item["duration"])

    # update using Welford's algorithm
    n += 1
    delta = x - mean
    mean_new = mean + delta / n
    delta2 = x - mean_new
    m2_new = m2 + delta * delta2

    # update df
    meta_df.at[idx, f"{suffix}-count"] = n
    meta_df.at[idx, f"{suffix}-mean"] = mean_new
    meta_df.at[idx, f"{suffix}-m2"] = m2_new
    
    return meta_df

def cold_start_meta(direction) -> pd.DataFrame:
    """Run cold start for the given direction. Loads all the data in DynamoDB to incrementally run update_item. Returns new meta df."""
    log.info("Running cold start for direction=%s", direction)

    # create new df with needed columns
    meta_df = pd.DataFrame({
        "hour": range(24),
        "wd-mean": 0.0,
        "wd-count": 0,
        "wd-m2": 0.0,
        "we-mean": 0.0,
        "we-count": 0,
        "we-m2": 0.0,
    })

    # get all data for direction
    data = get_all_direction(direction)

    # iterate through data, passing to update_item
    for item in data:
        meta_df = meta_update_item(meta_df, item)

    return meta_df

def get_all_direction(direction, return_df=False) -> dict | pd.DataFrame:
    """Return all stored items for the given direction from DynamoDB."""

    items = []
    last_key = None

    while True:
        kwargs = {
            "KeyConditionExpression": Key("route").eq(direction),
            "ScanIndexForward": True,
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    if return_df:
        df = pd.DataFrame(items)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["dt_eastern"] = df["timestamp"].dt.tz_convert("America/New_York")

        # 20 minute features
        df["dow_20m"] = df["dt_eastern"].dt.day_name()
        df["is_weekend_20m"] = df["dow_20m"].isin(["Saturday", "Sunday"])
        df["hhmm_20m"] = df["dt_eastern"].dt.round("20min").dt.strftime("%H:%M")

        # 1 hour features
        temp = df["dt_eastern"].dt.round("1h")
        df["dow_1h"] = temp.dt.day_name()
        df["is_weekend_1h"] = df["dow_1h"].isin(["Saturday", "Sunday"])
        df["hour_1h"] = temp.dt.round("1h").dt.hour
        return df
    
    # if not return dataframe
    return items

def get_s3_csv_file(file_name) -> pd.DataFrame | None:
    """Return pandas dataframe from S3, or None if file does not exist."""

    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_name)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise  # re-raise anything else (permissions, etc.)

    data = obj["Body"].read().decode("utf-8")
    df = pd.read_csv(io.StringIO(data))
    return df

def save_s3_csv_file(df, file_name):
    """Save pandas dataframe to S3."""
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=csv_buffer.getvalue())

def get_datetime_info(dt: str) -> tuple:
    """Return datetime info for the given datetime string. (dt_eastcoast, dt_ec_rounded, hour, is_weekend)"""
    dt_eastcoast = pd.to_datetime(dt).tz_convert("America/New_York")
    dt_ec_rounded = dt_eastcoast.round("1h")
    hour = dt_ec_rounded.hour
    is_weekend = dt_ec_rounded.day_name().lower() in ["saturday", "sunday"]
    return dt_eastcoast, dt_ec_rounded, hour, is_weekend


# Plot Helpers
def update_plots():
    # update past 24 hour plot
    try:
        n_24hr = get_past_24hr("north", shift_to_edt=True)
        s_24hr = get_past_24hr("south", shift_to_edt=True)
        if len(n_24hr) == 0 or len(s_24hr) == 0:
            log.warning("plot_24hr_skipped_no_data")
            return
        both = pd.concat([n_24hr, s_24hr], axis=0)

        log.info("Creating 24hr plot")

        fig_24hr = create_recent_plot(both)
        
        if fig_24hr is not None:
            save_fig_to_s3(fig_24hr, "plots/24hr.png")
            log.info("plot_24hr_saved")
        else:
            log.warning("plot_24hr_skipped_no_data")

        log.info("Finished saving 24hr plot")
    except Exception:
        log.exception("24hr plot failed")

    # update the aggregation plots
    try:
        log.info("Creating weekend aggregation plot")
        weekend_plot = create_aggregated_plot("we")
        
        if weekend_plot is not None:
            save_fig_to_s3(weekend_plot, "plots/weekend.png")
            log.info("Saved weekend aggregation plot")
        else:
            log.warning("plot_weekend_skipped_no_data")
    except Exception:
        log.exception("weekend plot failed")

    try:
        log.info("Creating weekday aggregation plot")
        weekday_plot = create_aggregated_plot("wd")

        if weekday_plot is not None:
            save_fig_to_s3(weekday_plot, "plots/weekday.png")
            log.info("Saved weekday aggregation plot")
        else:
            log.warning("plot_weekday_skipped_no_data")
    except Exception:
        log.exception("weekday plot failed")

def create_recent_plot(df):
    """Plot Change in Drive Time over past 24 hours"""
    from matplotlib import pyplot as plt
    import seaborn as sns
    if df.empty or len(df) < 2:
        # log.info("Not enough history to plot yet (%d point(s))", len(df))
        return None

    sns.set_theme(style="darkgrid", context="talk", font_scale=0.9)

    fig, ax = plt.subplots(figsize=(14, 6))

    # price
    sns.lineplot(data=df, x="timestamp", y="duration", ax=ax, hue="route", linewidth=2.5, zorder=2)

    # # highlight spikes
    # if "trend" in df.columns:
    #     upspikes = df[df["trend"].isin(["SPIKE_UP"])]
    #     downspikes = df[df["trend"].isin(["SPIKE_DOWN"])]
    # else:
    #     upspikes = pd.DataFrame()
    #     downspikes = pd.DataFrame()
    # if not upspikes.empty:
    #     ax.scatter(
    #         upspikes["timestamp"],
    #         upspikes["price_usd"],
    #         s=120,
    #         marker="^",
    #         color="red",
    #         label="Spike Up (> $2)",
    #         zorder=5
    #     )
    # if not downspikes.empty:
    #     ax.scatter(
    #         downspikes["timestamp"],
    #         downspikes["price_usd"],
    #         s=120,
    #         marker="v",
    #         color="green",
    #         label="Spike Down (< -$2)",
    #         zorder=5
    #     )

    ax.legend()

    ax.set_title(
        "Charlottesville Drive Time Past 24 Hours Between School of Data Science and 29/64 Interchange\n"
        f"Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )

    ax.set_ylabel("Duration (seconds)")
    # ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:.2f}"))
    ax.set_xlabel("Time (EDT)", labelpad=8)

    plt.rcParams['timezone'] = 'US/Eastern'
    sns.despine(ax=ax, top=True, right=True)
    fig.autofmt_xdate(rotation=25, ha="right")
    import matplotlib.dates as mdates
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
    plt.tight_layout()

    return fig

def create_aggregated_plot(day_type=str):
    """Plot aggregated data: mean/sd for each hour. takes in either 'we' or 'wd' """
    from matplotlib import pyplot as plt
    import seaborn as sns

    if day_type not in ["we", "wd"]:
        raise ValueError("day_type must be either 'we' or 'wd'")
    long_day_type = "Weekend" if day_type == "we" else "Weekday"
    
    # get metadata files
    north_meta_df = get_s3_csv_file("data/north_metadata.csv")
    south_meta_df = get_s3_csv_file("data/south_metadata.csv")

    # add sd
    north_meta_df[f"{day_type}-sd"] = np.where(
        north_meta_df[f"{day_type}-count"] > 1,
        (north_meta_df[f"{day_type}-m2"] / (north_meta_df[f"{day_type}-count"] - 1)) ** 0.5,
        0
    )
    south_meta_df[f"{day_type}-sd"] = np.where(
        south_meta_df[f"{day_type}-count"] > 1,
        (south_meta_df[f"{day_type}-m2"] / (south_meta_df[f"{day_type}-count"] - 1)) ** 0.5,
        0
    )

    # create plot: plot mean across hours with errorbars via 2*sd
    sns.set_theme(style="darkgrid", context="talk", font_scale=0.9)
    fig, ax = plt.subplots(figsize=(14, 6))

    sns.lineplot(data=north_meta_df, x="hour", y=f"{day_type}-mean", ax=ax, label="North")
    sns.lineplot(data=south_meta_df, x="hour", y=f"{day_type}-mean", ax=ax, label="South")

    ax.fill_between(north_meta_df["hour"], north_meta_df[f"{day_type}-mean"] - north_meta_df[f"{day_type}-sd"], north_meta_df[f"{day_type}-mean"] + north_meta_df[f"{day_type}-sd"], alpha=0.2)
    ax.fill_between(south_meta_df["hour"], south_meta_df[f"{day_type}-mean"] - south_meta_df[f"{day_type}-sd"], south_meta_df[f"{day_type}-mean"] + south_meta_df[f"{day_type}-sd"], alpha=0.2)

    # have x ticks every 2 hours
    ax.set_xticks(north_meta_df["hour"].unique()[::2])

    ax.set_title(f"{long_day_type} Mean Drive Time Across Hours Between School of Data Science and 29/64 Interchange\nLast updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    ax.set_ylabel("Duration (seconds)")
    ax.set_xlabel("Hour (ET)", labelpad=8)
    sns.despine(ax=ax, top=True, right=True)
    fig.autofmt_xdate(rotation=25, ha="right")
    plt.tight_layout()

    return fig

def get_past_24hr(direction, shift_to_edt=False):
    # query dynamodb
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(TABLE_NAME)

    cur_datetime = datetime.now(timezone.utc)
    now = cur_datetime.isoformat()
    start = (cur_datetime - timedelta(days=1)).isoformat()

    resp = table.query(
        KeyConditionExpression=Key("route").eq(direction) & Key("timestamp").between(start, now),
        ScanIndexForward=True,   # scending timestamp order
    )

    # create dataframe
    items = resp.get("Items", [])
    df = pd.DataFrame(items)

    df["timestamp"]   = pd.to_datetime(df["timestamp"])
    df["distance"] = df["distance"].astype(int)
    df["base_duration"] = df["base_duration"].astype(int) 
    df["duration"] = df["duration"].astype(int) 
    df["delta"] = pd.to_numeric(
        df.get("delta"), errors="coerce"
    ).fillna(0)

    if shift_to_edt:
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_convert("America/New_York")

    return df

def save_fig_to_s3(fig, filename):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    buf.seek(0)
    s3.put_object(Bucket=BUCKET_NAME, Key=filename, Body=buf.getvalue(), ContentType="image/png")


if __name__ == "__main__":
    # temp = get_all_direction("south", return_df=True)
    # print(temp[60:80])
    # print(len(temp))

     #update_plots()
     pass

    # handler(None, None)