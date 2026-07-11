"""Streaming memory-safe XML feed parser."""

from __future__ import annotations

import gzip
import logging
from pathlib import Path
from typing import Generator

from lxml import etree

logger = logging.getLogger(__name__)


class XMLFeedParser:
    """Stream XML documents element-by-element using lxml.iterparse."""

    def __init__(self, config) -> None:
        self.config = config

    def iter_jobs(self, file_path: Path) -> Generator[dict[str, str], None, None]:
        """Parse XML document, yield dictionary of job nodes, and release memory."""
        # Detect gzip compression by reading magic bytes
        is_gzipped = False
        try:
            with open(file_path, "rb") as f:
                is_gzipped = f.read(2) == b"\x1f\x8b"
        except Exception:
            pass

        opened_file = gzip.open(file_path, "rb") if is_gzipped else open(file_path, "rb")
        
        try:
            # Find the job/listing node elements
            context = etree.iterparse(opened_file, events=("end",), tag=("job", "listing"))
            for _, elem in context:
                job_data = self._element_to_dict(elem)
                yield job_data
                
                # Clear references to release memory immediately
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
        finally:
            opened_file.close()

    def _element_to_dict(self, element: etree._Element) -> dict[str, str]:
        """Convert an etree Element flat XML child elements into a Python dict."""
        job = {}
        for child in element:
            if child.tag is not None:
                # If there are subnodes (like CDATA or text), extract text safely
                job[child.tag] = child.text if child.text is not None else ""
        return job
