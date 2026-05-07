from chalice import Chalice, Response

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import os
import logging

app = Chalice(app_name='my-project')

REGION = "us-east-1"
TABLE_NAME = os.environ.get("TABLE_NAME", "escape-cville-table")

dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

# add logging
log = logging.getLogger()
log.setLevel(logging.INFO)


# ==============
# ROUTING
# ==============

@app.route('/')
def index():
    log.info("Handling / (index) request")
    return {
        "about": "Tracks drive time from School of Data Science to 29/64 interchange over time.\n\nplots show previous 24 hours, weekday average, and weekend average",
        "resources": ["current", "trend", "plot", "plot/weekday", "plot/weekend"],
    }

# give most recent N and S drive times
@app.route('/current')
def current():
    log.info("Handling /current request")
    try:
        n = get_most_recent("north")[0]
        s = get_most_recent("south")[0]

        time = dt_str_to_edt(n["timestamp"])
        n_duration = seconds_to_mmss(n["duration"])
        s_duration = seconds_to_mmss(s["duration"])

        resp = (
            f"Most recent datapoint @ {time}:\n"
            f"  North (29/64->SDS): {n_duration}\n"
            f"  South (SDS->29/64): {s_duration}\n"
        )
        return {"response": resp}
    
    except ValueError as e:
        log.warning(str(e))
        return Response(
            body={"error": str(e)},
            status_code=404
        )

    except RuntimeError as e:
        log.error(str(e))
        return Response(
            body={"error": "Database unavailable"},
            status_code=503
        )

    except Exception:
        log.exception("Unhandled error in /current")
        return Response(
            body={"error": "Internal server error"},
            status_code=500
        )

# Return Link to Current 24hr Plot
@app.route('/plot')
def plot_24hr():
    log.info("Handling /plot request")
    return {"response": "https://escape-cville-bucket.s3.us-east-1.amazonaws.com/plots/24hr.png"}

# return link to weekday plot
@app.route('/plot/weekday')
def plot_weekday():
    log.info("Handling /plot/weekday request")
    return {"response": "https://escape-cville-bucket.s3.us-east-1.amazonaws.com/plots/weekday.png"}

# return link to weekend plot
@app.route('/plot/weekend')
def plot_weekend():
    log.info("Handling /plot/weekend request")
    return {"response": "https://escape-cville-bucket.s3.us-east-1.amazonaws.com/plots/weekend.png"}

# give average delta of past hour and estimate 20 minutes in the future
@app.route('/trend')
def trend():
    log.info("Handling /trend request")
    try:
        n = get_most_recent("north", limit=3)
        s = get_most_recent("south", limit=3)

        # get_average_delta from n and s
        n_deltas = [x["delta"] for x in n]
        s_deltas = [x["delta"] for x in s]

        n_avg_delta = sum(n_deltas) / len(n_deltas)
        s_avg_delta = sum(s_deltas) / len(s_deltas)
        n_delta_inc_dec = "increasing" if n_avg_delta > 0 else "decreasing"
        s_delta_inc_dec = "increasing" if s_avg_delta > 0 else "decreasing"

        time = dt_str_to_edt(n[0]["timestamp"])
        current_dt = datetime.fromisoformat(n[0]["timestamp"])
        future_dt = current_dt + timedelta(minutes=20)
        future_time = dt_str_to_edt(future_dt.isoformat())
        
        
        future_n_duration = seconds_to_mmss(n[0]["duration"]+n_avg_delta)
        future_s_duration = seconds_to_mmss(s[0]["duration"]+s_avg_delta)

        resp = (
            f"Over Last Hour (through {time}):\n\n"

            f"    North (29/64->SDS) drive time {n_delta_inc_dec} by {abs(n_avg_delta):.2f}s every 20 minutes\n"
            f"      At current rate, by {future_time}, North drive expected to be {future_n_duration}\n\n"

            f"    South (SDS->29/64) drive time {s_delta_inc_dec} by {abs(s_avg_delta):.2f}s every 20 minutes\n"
            f"      At current rate, by {future_time}, South drive expected to be {future_s_duration}\n"
        )

        return {"response": resp}
    
    except ValueError as e:
        log.warning(str(e))
        return Response(
            body={"error": str(e)},
            status_code=404
        )

    except RuntimeError as e:
        log.exception("Database unavailable")
        return Response(
            body={"error": "Database unavailable"},
            status_code=503
        )

    except Exception:
        log.exception("Unhandled error in /trend")
        return Response(
            body={"error": "Internal server error"},
            status_code=500
        )



# ================
# HELPER FUNCTIONS
# ================

def get_most_recent(direction, limit=1):
    try:
        resp = table.query(
            KeyConditionExpression=Key("route").eq(direction),
            ScanIndexForward=False,   # descending timestamp order
            Limit=limit,
        )

        items = resp.get("Items", [])
        
        if not items:
            raise ValueError(f"No data found for direction={direction}")

        return items
    
    except ClientError as e:
        log.exception("DynamoDB query failed")
        raise RuntimeError("Database query failed") from e
    
    except ValueError:
        raise

    except Exception as e:
        log.exception("Unexpected error in get_most_recent")
        raise

def dt_str_to_edt(dt_str):
    try:
        dt = datetime.fromisoformat(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))

        dt_edt = dt.astimezone(ZoneInfo("America/New_York"))
        return str(dt_edt).rsplit(":", 2)[0]
    except Exception:
        log.exception(f"Unexpected error in dt_str_to_edt: {dt_str}")
        return f"UNKNOWN {dt_str}"

def seconds_to_mmss(seconds):
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    return "%d:%02d" % (m, s)


if __name__ == '__main__':
    print(current()['response'])
    print(trend()['response'])