import gzip
import json
import logging
from collections import Counter
import requests
from prefect import flow, task


URL = "https://data.gharchive.org/2025-01-01-{hour}.json.gz"

# log to a local file; Prefect already configures root logging, so basicConfig
# would be a no-op -- attach a file handler directly instead
logger = logging.getLogger("gharchive")
logger.setLevel(logging.INFO)
_fh = logging.FileHandler("gharchive_flow.log")
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(_fh)


@task(retries=3)
def count_events_for_hour(hour):
    # stream one hourly file and return {event_type: count}
    counts, skipped = Counter(), 0
    try:
        with requests.get(URL.format(hour=hour), stream=True, timeout=60) as r:
            r.raise_for_status()
            # decompress the stream on the fly; each line is one JSON event
            with gzip.GzipFile(fileobj=r.raw) as f:
                for line in f:
                    try:
                        counts[json.loads(line)["type"]] += 1
                    except (json.JSONDecodeError, KeyError):
                        skipped += 1
    except (requests.RequestException, OSError) as e:
        logger.error(f"hour {hour} failed: {e}")
        raise  # re-raise so Prefect retries the task
    if skipped:
        logger.warning(f"hour {hour}: skipped {skipped} malformed lines")
    print(f"hour {hour}: {counts.total()} events")
    logger.info(f"hour {hour}: {counts.total()} events")
    return dict(counts)


@task
def combine_counts(hourly_counts):
    # Total counts by type and order them
    # print those to screen and return them
    try:
        # Counter.update adds counts per key across every hourly dict
        totals = Counter()
        for counts in hourly_counts:
            totals.update(counts)
        ordered = dict(totals.most_common())  # sorted by count, highest first
    except (TypeError, AttributeError) as e:
        logger.error(f"combine_counts failed: {e}")
        raise
    print(f"total events across {len(hourly_counts)} hours: {totals.total()}")
    logger.info(f"combined totals: {ordered}")
    return ordered


@flow(log_prints=True)
def gharchive_flow(hours: list[int] = list(range(1, 4))):
    # take the range and create a prefect .map over the hours
    hourly = count_events_for_hour.map(hours).result()
    totals = combine_counts(hourly)
    for k, v in totals.items():
        print(k, v)


if __name__ == "__main__":
    gharchive_flow()
