from chalice import Chalice
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone, timedelta
import os

app = Chalice(app_name='my-project')

REGION = "us-east-1"
TABLE_NAME = os.environ.get("TABLE_NAME", "escape-cville-table")


# ==============
# ROUTING
# ==============

@app.route('/')
def index():
    return {
        "about": "Tracks drive time from School of Data Science to 29/64 interchange over time.\n\nplots show previous 24 hours, weekday average, and weekend average",
        "resources": ["current", "trend", "plot", "plot/weekday", "plot/weekend"],
    }

# give most recent N and S drive times
@app.route('/current')
def current():
    n = get_most_recent("north")[0]
    s = get_most_recent("south")[0]

    time = dt_str_to_edt(n["timestamp"])
    n_duration = seconds_to_mmss(n["duration"])
    s_duration = seconds_to_mmss(s["duration"])

    resp = f"Most recent datapoint @ {time}:\n  North (29/64->SDS): {n_duration}\n  South (SDS->29/64): {s_duration}\n"

    return {"response": resp}

# Return Link to Current 24hr Plot
@app.route('/plot')
def plot():
    return {"response": "https://escape-cville-bucket.s3.us-east-1.amazonaws.com/plots/24hr.png"}

# return link to weekday plot
@app.route('/plot/weekday')
def plot():
    return {"response": "https://escape-cville-bucket.s3.us-east-1.amazonaws.com/plots/weekday.png"}

# return link to weekend plot
@app.route('/plot/weekend')
def plot():
    return {"response": "https://escape-cville-bucket.s3.us-east-1.amazonaws.com/plots/weekend.png"}

# give ??
@app.route('/trend')
def trend():
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
    future_time = dt_str_to_edt(str(datetime.fromisoformat(n[0]["timestamp"]) + timedelta(minutes=20)))
    
    
    future_n_duration = seconds_to_mmss(n[0]["duration"]+n_avg_delta)
    future_s_duration = seconds_to_mmss(s[0]["duration"]+s_avg_delta)

    resp = f"Over Last Hour (through {time}):\n\n    North (29/64->SDS) drive time {n_delta_inc_dec} by {abs(n_avg_delta):.2f}s every 20 minutes\n      At current rate, by {future_time}, North drive expected to be {future_n_duration}\n\n    South (SDS->29/64) drive time {s_delta_inc_dec} by {abs(s_avg_delta):.2f}s every 20 minutes\n      At current rate, by {future_time}, South drive expected to be {future_s_duration}\n"

    return {"response": resp}



# ================
# HELPER FUNCTIONS
# ================

def get_most_recent(direction, limit=1):
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(TABLE_NAME)

    resp = table.query(
        KeyConditionExpression=Key("route").eq(direction),
        ScanIndexForward=False,   # descending timestamp order
        Limit=limit,
    )

    items = resp.get("Items", [])
    return items if items else None

def dt_str_to_edt(dt_str):
    dt = datetime.fromisoformat(dt_str)
    dt_edt = dt.astimezone(timezone(timedelta(hours=-4)))
    return str(dt_edt).rsplit(":", 2)[0]

def seconds_to_mmss(seconds):
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    return "%d:%02d" % (m, s)


if __name__ == '__main__':
    print(current()['response'])
    print(trend()['response'])