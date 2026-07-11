"""Configuration module defining global parameters for the Feed Merger CLI."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class MergerConfig:
    """Holds configuration parameters for the pipeline execution."""

    # File paths
    feeds_file: Path = Path("feeds.txt")
    output_file: Path = Path("output/merged.xml")
    duplicate_db: Path = Path("duplicates.db")
    statistics_file: Path = Path("stats.json")

    # Directories
    downloads_dir: Path = Path("downloads")
    logs_dir: Path = Path("logs")
    temp_dir: Path = Path("downloads/tmp")

    # Network parameters
    chunk_size: int = 1024 * 1024  # 1 MB chunk streaming
    timeout_seconds: int = 600  # 10 minutes timeout for large feeds
    retry_count: int = 3
    max_concurrent_downloads: int = 3

    # Pipeline configurations
    duplicate_fields: list[str] = field(
        default_factory=lambda: ["title", "company", "location", "description"]
    )
    delete_temp_files: bool = True
    reset_duplicate_db: bool = False
    root_output_node: str = "source"  # Root XML node name

    # Standard browser headers to bypass server scrape blockers
    request_headers: dict[str, str] = field(
        default_factory=lambda: {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
    )
