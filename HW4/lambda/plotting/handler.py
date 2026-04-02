import boto3
import matplotlib
import os
from boto3.dynamodb.conditions import Key

matplotlib.use("Agg")  # non-interactive backend for Lambda
import io
import json
from datetime import datetime, timedelta, timezone
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
PLOT_KEY = "plot"
# Query window and x-axis span (title should match this)
PLOT_WINDOW_MINUTES = 5


def get_global_max(table, bucket_name):

    response = table.query(
        IndexName='bucket-size-index',
        KeyConditionExpression=Key("bucket_name").eq(bucket_name),
        ScanIndexForward=False,  # sort by total_size descending
        Limit=1  # only need the top item
    )
    items = response.get("Items", [])
    if items:
        return int(items[0]["total_size"])
    return 0


def handler(event, context):
    # get the bucket_name and table_name from environment variables
    bucket_name = os.environ['BUCKET_NAME']
    table_name = os.environ['TABLE_NAME']

    table = ddb.Table(table_name)

    # 1. Query last 5 minutes of data for TestBucket (wider window than 10s so
    #    driver + SQS/Lambda lag still shows multiple points on the plot)
    now = datetime.now(timezone.utc)
    window_start = (now - timedelta(minutes=PLOT_WINDOW_MINUTES)).isoformat()

    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key("bucket_name").eq(
            bucket_name
        )
        & boto3.dynamodb.conditions.Key("timestamp").gte(window_start)
    )
    recent_items = response["Items"]

    # 2. Find global max size across ALL buckets (query each bucket key)
    known_buckets = os.environ.get('KNOWN_BUCKETS', bucket_name).split(',')
    global_max = max(get_global_max(table, b) for b in known_buckets)

    #  3. Build plot 
    if not recent_items:
        # Nothing to plot yet — create an empty plot with a message
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.text(
            0.5,
            0.5,
            f"No data in last {PLOT_WINDOW_MINUTES} minutes",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=14,
        )
        ax.set_title(f"S3 Bucket Size Change (last {PLOT_WINDOW_MINUTES} min)")
        ax.set_xlim(now - timedelta(minutes=PLOT_WINDOW_MINUTES), now)
    else:
        # Sort by timestamp
        recent_items.sort(key=lambda x: x["timestamp"])

        timestamps = [
            datetime.fromisoformat(item["timestamp"]) for item in recent_items
        ]
        sizes = [int(item["total_size"]) for item in recent_items]

        fig, ax = plt.subplots(figsize=(10, 5))

        # Bucket size over time
        ax.plot(
            timestamps,
            sizes,
            marker="o",
            linewidth=2,
            label=f"{bucket_name} size",
            color="steelblue",
        )

        # Max size horizontal line
        ax.axhline(
            y=global_max,
            color="red",
            linestyle="--",
            linewidth=1.5,
            label=f"Global max: {global_max} bytes",
        )

        ax.set_title(f"S3 Bucket Size Change (last {PLOT_WINDOW_MINUTES} minutes)")
        ax.set_xlabel("Timestamp (UTC)")
        ax.set_ylabel("Total Size (bytes)")
        # Show full window on x-axis; otherwise matplotlib only spans min→max of points (~1 min of driver burst).
        ax.set_xlim(now - timedelta(minutes=PLOT_WINDOW_MINUTES), now)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        fig.autofmt_xdate()
        ax.legend()
        ax.grid(True, linestyle="--", alpha=0.5)

    plt.tight_layout()

    # ── 4. Save plot to S3 ───────────────────────────────────────────────────
    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    plt.close(fig)

    s3.put_object(
        Bucket=bucket_name,
        Key=PLOT_KEY,
        Body=buf.getvalue(),
        ContentType="image/png",
    )

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Plot saved to S3", "key": PLOT_KEY}),
        "headers": {"Content-Type": "application/json"},
    }
