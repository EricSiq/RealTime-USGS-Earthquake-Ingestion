"""
USGS Real-Time Earthquake Streaming Ingestor
Fetches live GeoJSON seismic events, deduplicates, and partitions raw files
into HDFS-compliant date/hour hierarchies:
/raw/earthquakes/YYYY/MM/DD/HH/quakes_<timestamp>.json
"""

import os
import sys
import json
import time
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Tuple, List

# USGS GeoJSON Endpoints
USGS_ALL_HOUR = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
USGS_ALL_DAY = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
USGS_ALL_WEEK = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_HDFS_DIR = DATA_DIR / "hdfs" / "raw" / "earthquakes"
STATE_FILE = DATA_DIR / "ingestion_state.json"

class USGSStreamer:
    def __init__(self, feed_url: str = USGS_ALL_DAY):
        self.feed_url = feed_url
        self.seen_ids = self._load_state()
        RAW_HDFS_DIR.mkdir(parents=True, exist_ok=True)

    def _load_state(self) -> set:
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return set(data.get("seen_ids", []))
            except Exception as e:
                print(f"[WARN] Failed to read state file: {e}")
        return set()

    def _save_state(self):
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        # Keep recent 20,000 IDs to prevent state bloat
        recent_ids = list(self.seen_ids)[-20000:]
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "last_run": datetime.now(timezone.utc).isoformat(),
                "seen_ids": recent_ids,
                "total_unique_seen": len(self.seen_ids)
            }, f, indent=2)

    def fetch_feed(self) -> Dict[str, Any]:
        headers = {"User-Agent": "BDA-Hadoop-Seismic-Pipeline/1.0"}
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Connecting to USGS API: {self.feed_url}")
        resp = requests.get(self.feed_url, headers=headers, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def partition_and_land(self, payload: Dict[str, Any]) -> Tuple[int, int, List[Path]]:
        features = payload.get("features", [])
        new_features = []

        for feat in features:
            fid = feat.get("id")
            if fid and fid not in self.seen_ids:
                new_features.append(feat)
                self.seen_ids.add(fid)

        if not new_features:
            print("  No new events detected since last cycle.")
            return 0, len(features), []

        now = datetime.now(timezone.utc)
        year_str = f"YYYY={now.strftime('%Y')}"
        month_str = f"MM={now.strftime('%m')}"
        day_str = f"DD={now.strftime('%d')}"
        hour_str = f"HH={now.strftime('%H')}"

        target_dir = RAW_HDFS_DIR / year_str / month_str / day_str / hour_str
        target_dir.mkdir(parents=True, exist_ok=True)

        batch_ts = now.strftime("%Y%m%d_%H%M%S")
        out_file = target_dir / f"usgs_quakes_{batch_ts}.json"

        # Save partitioned raw landing file
        batch_payload = {
            "metadata": payload.get("metadata", {}),
            "ingested_at": now.isoformat(),
            "batch_count": len(new_features),
            "features": new_features
        }

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(batch_payload, f, indent=2)

        # Also save individual line-delimited JSON for Hive JsonSerDe direct query
        ndjson_file = target_dir / f"usgs_quakes_{batch_ts}.jsonl"
        with open(ndjson_file, "w", encoding="utf-8") as f:
            for feat in new_features:
                f.write(json.dumps(feat) + "\n")

        self._save_state()
        print(f"  Successfully landed {len(new_features)} new events to HDFS raw landing zone:")
        print(f"    -> {out_file.relative_to(BASE_DIR)}")

        return len(new_features), len(features), [out_file, ndjson_file]

    def run_once(self) -> Tuple[int, int]:
        data = self.fetch_feed()
        new_cnt, total_cnt, _ = self.partition_and_land(data)
        return new_cnt, total_cnt

if __name__ == "__main__":
    feed = USGS_ALL_DAY
    if len(sys.argv) > 1 and sys.argv[1] == "--week":
        feed = USGS_ALL_WEEK
    elif len(sys.argv) > 1 and sys.argv[1] == "--hour":
        feed = USGS_ALL_HOUR

    streamer = USGSStreamer(feed_url=feed)
    new_cnt, total_cnt = streamer.run_once()
    print(f"Ingestion complete: {new_cnt} newly ingested / {total_cnt} polled.")
