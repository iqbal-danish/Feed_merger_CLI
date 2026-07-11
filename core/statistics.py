"""Aggregator for pipeline performance statistics."""

from __future__ import annotations

import json
import time
from pathlib import Path
import psutil


class MergeStatistics:
    """Collects and serializes telemetry data of the merging pipeline."""

    def __init__(self) -> None:
        self.started_at: float = time.perf_counter()
        self.total_feeds: int = 0
        self.successful_feeds: int = 0
        self.failed_feeds: int = 0
        self.jobs_parsed: int = 0
        self.jobs_written: int = 0
        self.duplicates_removed: int = 0
        self.feeds: dict[str, dict] = {}  # Tracks individual feed details

    def snapshot(self) -> dict:
        """Generate a dictionary representation of the current metrics."""
        elapsed = time.perf_counter() - self.started_at
        memory_rss = psutil.Process().memory_info().rss / (1024 * 1024)  # MB
        
        # Calculate CPU usage
        cpu_percent = 0.0
        try:
            cpu_percent = psutil.Process().cpu_percent(interval=None)
        except Exception:
            pass

        return {
            "total_feeds": self.total_feeds,
            "successful_feeds": self.successful_feeds,
            "failed_feeds": self.failed_feeds,
            "jobs_parsed": self.jobs_parsed,
            "jobs_written": self.jobs_written,
            "duplicates_removed": self.duplicates_removed,
            "feeds": self.feeds,
            "elapsed_seconds": elapsed,
            "memory_rss_mb": memory_rss,
            "cpu_percent": cpu_percent,
            "jobs_per_second": self.jobs_written / elapsed if elapsed > 0 else 0.0,
        }

    def write_json(self, output_path: Path) -> None:
        """Write the statistics snapshot to a JSON file."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(self.snapshot(), f, indent=2)
