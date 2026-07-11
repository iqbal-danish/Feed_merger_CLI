"""SQLite database index deduplicator for streaming job nodes."""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path
from types import TracebackType


class SQLiteDeduplicator:
    """Uses SQLite to index hashes of job fields and instantly deduplicate items."""

    def __init__(self, db_path: Path, duplicate_fields: list[str]) -> None:
        self.db_path = db_path
        self.duplicate_fields = duplicate_fields
        self.conn: sqlite3.Connection | None = None
        self.cursor: sqlite3.Cursor | None = None

    def __enter__(self) -> SQLiteDeduplicator:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        
        # Optimize SQLite for speed (offload write syncing safety for speed)
        self.cursor.execute("PRAGMA synchronous = OFF")
        self.cursor.execute("PRAGMA journal_mode = MEMORY")
        
        # Hash index table
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS job_hashes (
                hash TEXT PRIMARY KEY
            )
            """
        )
        self.conn.commit()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self.conn:
            self.conn.close()

    def seen(self, job_element: dict[str, str]) -> bool:
        """Check if job hash is already stored. If not, insert and return False."""
        hash_val = self._generate_hash(job_element)
        if not hash_val:
            return False

        if not self.cursor or not self.conn:
            raise RuntimeError("Database connection is not open. Use with block.")

        self.cursor.execute(
            "SELECT 1 FROM job_hashes WHERE hash = ?", (hash_val,)
        )
        if self.cursor.fetchone():
            return True

        self.cursor.execute(
            "INSERT INTO job_hashes (hash) VALUES (?)", (hash_val,)
        )
        self.conn.commit()
        return False

    def _generate_hash(self, job_element: dict[str, str]) -> str | None:
        """Generate a SHA-256 hash using the configured duplicate check fields."""
        hasher = hashlib.sha256()
        has_content = False

        for field_name in self.duplicate_fields:
            val = job_element.get(field_name, "").strip().lower()
            if val:
                hasher.update(val.encode("utf-8"))
                has_content = True

        return hasher.hexdigest() if has_content else None
