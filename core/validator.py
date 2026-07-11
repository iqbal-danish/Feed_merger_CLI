"""XML syntax validator helper."""

from __future__ import annotations

import logging
from pathlib import Path

from lxml import etree

logger = logging.getLogger(__name__)


class XMLValidator:
    """Validate standard XML structural well-formedness."""

    def validate_file(self, file_path: Path) -> bool:
        """Parse XML file to check for malformed syntax. Returns True if well-formed."""
        try:
            etree.parse(str(file_path))
            logger.info("XML Validation passed successfully: %s", file_path.name)
            return True
        except etree.XMLSyntaxError as exc:
            logger.error("XML Validation failed for %s: %s", file_path, exc)
            raise exc
        except Exception as exc:
            logger.error("Failed to parse file for validation %s: %s", file_path, exc)
            raise exc
