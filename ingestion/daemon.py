"""
USGS Ingestion Daemon
Simulates a production cron / streaming ingestion loop that periodically polls USGS feeds,
writes partitioned chunks into HDFS, and logs streaming throughput.
"""

import time
import argparse
from datetime import datetime
from usgs_streamer import USGSStreamer, USGS_ALL_HOUR, USGS_ALL_DAY, USGS_ALL_WEEK

def start_daemon(interval_sec: int = 60, cycles: int = 5, feed_type: str = "day"):
    feed_url = USGS_ALL_DAY
    if feed_type == "hour":
        feed_url = USGS_ALL_HOUR
    elif feed_type == "week":
        feed_url = USGS_ALL_WEEK

    print("=" * 65)
    print(f"Starting USGS Ingestion Daemon")
    print(f"Feed: {feed_url}")
    print(f"Poll Interval: {interval_sec} seconds | Target Cycles: {cycles or 'Continuous'}")
    print("=" * 65)

    streamer = USGSStreamer(feed_url=feed_url)
    completed = 0
    total_ingested = 0

    try:
        while True:
            completed += 1
            print(f"\n[Cycle {completed}] Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            new_cnt, total_cnt = streamer.run_once()
            total_ingested += new_cnt

            if cycles and completed >= cycles:
                print("\nReached target cycle limit. Daemon stopping.")
                break

            print(f"Sleeping for {interval_sec} seconds before next poll...")
            time.sleep(interval_sec)

    except KeyboardInterrupt:
        print("\nDaemon interrupted by user.")

    print("=" * 65)
    print(f"Daemon Summary: {completed} cycles completed, {total_ingested} total new events landed.")
    print("=" * 65)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run USGS Streaming Ingestion Daemon")
    parser.add_argument("--interval", type=int, default=60, help="Polling interval in seconds")
    parser.add_argument("--cycles", type=int, default=1, help="Number of polling cycles (0 for infinite)")
    parser.add_argument("--feed", choices=["hour", "day", "week"], default="week", help="USGS feed window")
    args = parser.parse_args()

    start_daemon(interval_sec=args.interval, cycles=args.cycles, feed_type=args.feed)
