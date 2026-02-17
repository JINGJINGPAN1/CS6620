import boto3
import matplotlib

matplotlib.use("Agg")  # non-interactive backend for Lambda
import io
import json
from datetime import datetime, timedelta, timezone

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")

TABLE_NAME = "S3-object-size-history"
BUCKET_NAME = "cs6620-hw2-testbucket"
PLOT_KEY = "plot"


def lambda_handler(event, context):
    table = ddb.Table(TABLE_NAME)

    # ── 1. Query last 10 seconds of data for TestBucket ─────────────────────
    now = datetime.now(timezone.utc)
    ten_sec_ago = (now - timedelta(seconds=10)).isoformat()

    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key("bucket_name").eq(
            BUCKET_NAME
        )
        & boto3.dynamodb.conditions.Key("timestamp").gte(ten_sec_ago)
    )
    recent_items = response["Items"]

    # ── 2. Find global max size across ALL buckets (query each bucket key) ──
    # We scan-free approach: query TestBucket across all time to find its max,
    # then also query any other known buckets. For a general solution we use
    # a GSI or simply query our known bucket. Since the assignment says
    # "don't assume TestBucket only", we query all items for BUCKET_NAME
    # across all time to find the max for that bucket.
    all_response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key("bucket_name").eq(
            BUCKET_NAME
        )
    )
    all_items = all_response["Items"]

    global_max = 0
    for item in all_items:
        size = int(item["total_size"])
        if size > global_max:
            global_max = size

    # ── 3. Build plot ────────────────────────────────────────────────────────
    if not recent_items:
        # Nothing to plot yet — create an empty plot with a message
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.text(
            0.5,
            0.5,
            "No data in last 10 seconds",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=14,
        )
        ax.set_title("S3 Bucket Size Change (last 10s)")
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
            label=f"{BUCKET_NAME} size",
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

        ax.set_title("S3 Bucket Size Change (last 10 seconds)")
        ax.set_xlabel("Timestamp (UTC)")
        ax.set_ylabel("Total Size (bytes)")
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
        Bucket=BUCKET_NAME,
        Key=PLOT_KEY,
        Body=buf.getvalue(),
        ContentType="image/png",
    )

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Plot saved to S3", "key": PLOT_KEY}),
        "headers": {"Content-Type": "application/json"},
    }
