"""Command-line entry point for the XML Feed Merger application."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from rich.console import Console

from config import MergerConfig
from core.merger import FeedMerger

console = Console()

# Hardcode your primary feed URLs or local file paths here.
# Running "python app.py" with no arguments will automatically merge these sources.
DEFAULT_FEEDS = [
    "https://www.ziprecruiter.com/feed/cpc_monstercareerbuilder_priority.xml",
    "https://www.ziprecruiter.com/feed/cpc_monstercareerbuilder_standard.xml",
]


def configure_logging(logs_dir: Path) -> None:
    """Setup file-only logging to keep the terminal console clean for progress indicators."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "merger.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
        force=True,
    )


def parse_args() -> argparse.Namespace:
    """Configure and parse CLI command-line arguments."""
    parser = argparse.ArgumentParser(
        description="XML Feed Merger CLI: Memory-safe concurrent job feed merging and deduplication."
    )
    parser.add_argument(
        "feeds",
        type=str,
        nargs="*",
        help="Space-separated list of feed URLs or local XML file paths to merge (overrides defaults)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/merged.xml"),
        help="Path to save the merged output XML file (default: output/merged.xml)",
    )
    parser.add_argument(
        "--duplicates-db",
        type=Path,
        default=Path("duplicates.db"),
        help="Path to the SQLite deduplication database (default: duplicates.db)",
    )
    parser.add_argument(
        "--reset-db",
        action="store_true",
        help="Reset/clear the SQLite duplicate database index on startup",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Do not delete temporary downloaded XML files on completion",
    )
    parser.add_argument(
        "--fields",
        type=str,
        default="title,company,location,description",
        help="Comma-separated list of XML sub-node elements to use for duplicate checks",
    )
    return parser.parse_args()


async def main() -> None:
    """CLI execution main function."""
    args = parse_args()

    # Load custom properties into MergerConfig
    config = MergerConfig(
        output_file=args.output,
        duplicate_db=args.duplicates_db,
        reset_duplicate_db=args.reset_db,
        delete_temp_files=not args.keep_temp,
        duplicate_fields=[f.strip() for f in args.fields.split(",") if f.strip()],
    )

    # Enable background log file tracking
    configure_logging(config.logs_dir)
    
    console.print("[bold green]XML Feed Merger CLI[/] | Terminal Pipeline Orchestration")
    console.print("-" * 65)

    # Get feeds list from arguments or fall back to DEFAULT_FEEDS
    feeds = list(args.feeds)
    if not feeds:
        console.print("[bold yellow]No custom feeds specified. Merging default sources...[/]")
        feeds = DEFAULT_FEEDS
        
        if not feeds:
            console.print("[bold red]Error: No default feed sources configured in code. Exiting.[/]")
            sys.exit(1)
            
        for f in feeds:
            console.print(f" • [cyan]{f}[/]")
        console.print("-" * 65)

    merger = FeedMerger(config)
    try:
        await merger.run(feeds)
        
        # Display completion telemetry card
        stats = merger.statistics.snapshot()
        console.print("\n[bold green]✓ Pipeline Execution Complete![/]")
        console.print("-" * 65)
        console.print(f"[bold]Output XML File:[/]   [cyan]{config.output_file}[/]")
        console.print(f"[bold]Feeds Processed:[/]  [green]{stats['successful_feeds']} successful[/] / {stats['total_feeds']} total")
        console.print(f"[bold]Jobs Merged:[/]      [yellow]{stats['jobs_written']:,}[/]")
        console.print(f"[bold]Dupes Removed:[/]    [red]{stats['duplicates_removed']:,}[/]")
        console.print(f"[bold]Time Elapsed:[/]     {stats['elapsed_seconds']:.2f}s")
        console.print(f"[bold]Average Speed:[/]    {stats['jobs_per_second']:.1f} jobs/sec")
        console.print("-" * 65)
    except Exception as exc:
        logging.exception("Fatal pipeline execution failure:")
        console.print(f"\n[bold red]✕ Pipeline failed: {exc}[/]")
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Pipeline execution interrupted by user.[/]")
        sys.exit(0)
