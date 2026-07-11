"""Streaming downloader helper for HTTP/HTTPS feeds with progress callbacks."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from pathlib import Path
from typing import Callable
from urllib.parse import urlparse

import aiohttp

from config import MergerConfig

logger = logging.getLogger(__name__)


class DownloadError(RuntimeError):
    """Raised when a feed cannot be downloaded after retries."""


class FeedDownloader:
    """Download remote HTTP/HTTPS feeds to disk with progress indicators."""

    def __init__(self, config: MergerConfig) -> None:
        self.config = config

    async def download(
        self,
        url: str,
        progress_callback: Callable[[int], None] | None = None,
        size_callback: Callable[[int], None] | None = None,
    ) -> Path:
        """Stream a remote source into a temporary file and return its path."""
        self.config.temp_dir.mkdir(parents=True, exist_ok=True)
        
        target = self._target_path(url)
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
        last_error: Exception | None = None

        for attempt in range(1, self.config.retry_count + 1):
            partial = target.with_suffix(f"{target.suffix}.part{attempt}")
            try:
                async with aiohttp.ClientSession(
                    timeout=timeout,
                    headers=self.config.request_headers,
                    auto_decompress=False,
                ) as session:
                    logger.info("Download started: %s", url)
                    async with session.get(url) as response:
                        response.raise_for_status()
                        
                        # Fetch size if provided by server
                        content_length = response.headers.get("Content-Length")
                        if content_length and size_callback:
                            try:
                                size_callback(int(content_length))
                            except ValueError:
                                pass
                                
                        bytes_written = 0
                        with partial.open("wb") as file:
                            async for chunk in response.content.iter_chunked(
                                self.config.chunk_size
                            ):
                                if not chunk:
                                    continue
                                file.write(chunk)
                                bytes_written += len(chunk)
                                if progress_callback:
                                    progress_callback(len(chunk))

                # Retry rename on Windows to handle antivirus/indexing locks
                for rename_attempt in range(15):
                    try:
                        os.replace(partial, target)
                        break
                    except OSError as rename_err:
                        if rename_attempt == 14:
                            raise rename_err
                        await asyncio.sleep(0.3)

                logger.info(
                    "Download completed: %s -> %s (%s bytes)",
                    url,
                    target,
                    bytes_written,
                )
                return target
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
                last_error = exc
                try:
                    partial.unlink(missing_ok=True)
                except Exception:
                    pass
                logger.warning(
                    "Download attempt %s/%s failed for %s: %s",
                    attempt,
                    self.config.retry_count,
                    url,
                    exc,
                )
                if attempt < self.config.retry_count:
                    await asyncio.sleep(min(2 ** (attempt - 1), 10))

        raise DownloadError(f"Failed to download {url}") from last_error

    def _target_path(self, url: str) -> Path:
        parsed = urlparse(url)
        suffixes = "".join(Path(parsed.path).suffixes)
        suffix = suffixes if suffixes.lower().endswith((".xml", ".xml.gz", ".gz")) else ".xml"
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
        return self.config.temp_dir / f"{digest}{suffix}"
