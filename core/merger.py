"""Pipeline coordinator executing downloads, parsing, and writing with CLI progress bars."""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from urllib.parse import urlparse

from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TaskID,
)

from config import MergerConfig
from core.deduplicator import SQLiteDeduplicator
from core.downloader import FeedDownloader
from core.parser import XMLFeedParser
from core.statistics import MergeStatistics
from core.validator import XMLValidator
from core.writer import XMLStreamWriter

logger = logging.getLogger(__name__)


class FeedMerger:
    """Coordinate downloads, parsing, deduplication, writing, and validation with visual progress."""

    def __init__(self, config: MergerConfig) -> None:
        self.config = config
        self.downloader = FeedDownloader(config)
        self.parser = XMLFeedParser(config)
        self.validator = XMLValidator()
        self.statistics = MergeStatistics()

    async def run(self, sources: list[str]) -> None:
        """Merge all feed sources provided."""
        self._prepare_directories()
        if self.config.reset_duplicate_db:
            self.config.duplicate_db.unlink(missing_ok=True)
            
        if not sources:
            logger.warning("No feed sources provided.")
            return

        self.statistics.total_feeds = len(sources)
        
        for src in sources:
            self.statistics.feeds[src] = {
                "status": "pending",
                "file_size_bytes": 0,
                "jobs_parsed": 0,
                "jobs_written": 0,
                "elapsed_seconds": 0.0,
            }
            
        logger.info("Starting merge for %s feed source(s)", len(sources))

        # Concurrent downloads for remote sources (standard URLs)
        remote_sources = [src for src in sources if src.startswith(("http://", "https://"))]
        download_map: dict[str, Path] = {}
        
        if remote_sources:
            download_columns = (
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(bar_width=40),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeElapsedColumn(),
            )
            
            with Progress(*download_columns) as progress:
                tasks = {
                    url: progress.add_task(f"Downloading {self._display_name(url)}", total=None)
                    for url in remote_sources
                }
                
                semaphore = asyncio.Semaphore(self.config.max_concurrent_downloads)
                
                async def _download_with_sem(url: str) -> tuple[str, Path | Exception]:
                    self.statistics.feeds[url]["status"] = "downloading"
                    
                    def progress_callback(chunk_len: int):
                        progress.update(tasks[url], advance=chunk_len)
                        
                    def size_callback(size: int):
                        progress.update(tasks[url], total=size)

                    async with semaphore:
                        try:
                            temp_path = await self.downloader.download(url, progress_callback, size_callback)
                            return url, temp_path
                        except Exception as exc:
                            return url, exc
                
                download_tasks = [_download_with_sem(url) for url in remote_sources]
                results = await asyncio.gather(*download_tasks)
                for url, result in results:
                    if isinstance(result, Exception):
                        self.statistics.failed_feeds += 1
                        self.statistics.feeds[url]["status"] = "failed"
                        logger.error("Failed to download feed %s: %s", url, result)
                    else:
                        download_map[url] = result
                        try:
                            self.statistics.feeds[url]["file_size_bytes"] = result.stat().st_size
                        except Exception:
                            pass

        merge_columns = (
            SpinnerColumn(),
            TextColumn("[bold green]{task.description}"),
            BarColumn(bar_width=40),
            TextColumn("[yellow]{task.completed} jobs merged"),
            TimeElapsedColumn(),
        )

        # Deduplicate and stream merge
        with SQLiteDeduplicator(self.config.duplicate_db, self.config.duplicate_fields) as dedupe:
            with XMLStreamWriter(self.config.output_file, self.config.root_output_node) as writer:
                with Progress(*merge_columns) as progress:
                    for src in sources:
                        task_id = progress.add_task(f"Merging {self._display_name(src)}", total=None)
                        await self._process_source(src, download_map, dedupe, writer, progress, task_id)

        self.validator.validate_file(self.config.output_file)
        self.statistics.write_json(self.config.statistics_file)
        
        stats = self.statistics.snapshot()
        logger.info("Merge complete! Total Processed: %s/%s. Jobs Merged: %s (Duplicates Removed: %s).",
                    stats["successful_feeds"], stats["total_feeds"], stats["jobs_written"], stats["duplicates_removed"])

    async def _process_source(
        self,
        source: str,
        download_map: dict[str, Path],
        dedupe: SQLiteDeduplicator,
        writer: XMLStreamWriter,
        progress: Progress,
        task_id: TaskID,
    ) -> None:
        temp_path: Path | None = None
        started_at = time.perf_counter()
        self.statistics.feeds[source]["status"] = "processing"
        
        try:
            if source.startswith(("http://", "https://")):
                if source not in download_map:
                    self.statistics.feeds[source]["status"] = "failed"
                    progress.update(task_id, description=f"[red]Failed download: {self._display_name(source)}[/]")
                    return
                path = download_map[source]
                temp_path = path
            else:
                path = Path(source)
                if not path.exists():
                    raise FileNotFoundError(f"Local feed file does not exist: {path}")
                try:
                    self.statistics.feeds[source]["file_size_bytes"] = path.stat().st_size
                except Exception:
                    pass

            feed_jobs_parsed = 0
            feed_jobs_written = 0

            logger.info("Processing feed: %s", source)
            for job in self.parser.iter_jobs(path):
                self.statistics.jobs_parsed += 1
                feed_jobs_parsed += 1
                self.statistics.feeds[source]["jobs_parsed"] = feed_jobs_parsed

                if dedupe.seen(job):
                    self.statistics.duplicates_removed += 1
                    continue
                writer.write_element(job)
                self.statistics.jobs_written += 1
                feed_jobs_written += 1
                self.statistics.feeds[source]["jobs_written"] = feed_jobs_written
                
                # Advance rich progress bar task
                progress.update(task_id, advance=1)

            self.statistics.successful_feeds += 1
            self.statistics.feeds[source]["status"] = "completed"
            progress.update(task_id, description=f"[bold green]Merged {self._display_name(source)}[/]")
            logger.info(
                "Completed feed %s: parsed=%s written=%s elapsed=%.2fs",
                source,
                feed_jobs_parsed,
                feed_jobs_written,
                time.perf_counter() - started_at,
            )
        except Exception as exc:
            self.statistics.failed_feeds += 1
            self.statistics.feeds[source]["status"] = "failed"
            progress.update(task_id, description=f"[red]Failed {self._display_name(source)}: {str(exc)}[/]")
            logger.exception("Failed to process feed source: %s", source)
        finally:
            self.statistics.feeds[source]["elapsed_seconds"] = time.perf_counter() - started_at
            if temp_path and self.config.delete_temp_files:
                try:
                    temp_path.unlink(missing_ok=True)
                except Exception as clean_err:
                    logger.warning("Failed to clean up temp file %s: %s", temp_path, clean_err)

    def _display_name(self, src: str) -> str:
        if src.startswith(("http://", "https://")):
            return Path(urlparse(src).path).name or src
        return Path(src).name

    def _prepare_directories(self) -> None:
        for path in (
            self.config.output_file.parent,
            self.config.downloads_dir,
            self.config.logs_dir,
            self.config.temp_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
