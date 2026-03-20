"""SQLite persistence for downloaded dataset metadata."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


class MetadataStore:
    """Persists dataset metadata and download references into SQLite."""

    def __init__(self, db_path: str, logger) -> None:
        self._logger = logger
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = self._connect_sqlite(self.db_path)
        self._ensure_schema()

    def _connect_sqlite(self, db_path: Path) -> sqlite3.Connection:
        """Open a SQLite connection and recover if the target file is not SQLite."""
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute("PRAGMA schema_version").fetchone()
            return conn
        except sqlite3.DatabaseError as exc:
            conn.close()
            backup_path = self._rotate_invalid_database(db_path)
            self._logger.warning(
                "Database file %s is not a SQLite database (%s). Moved to %s and created a new SQLite file.",
                db_path,
                exc,
                backup_path,
            )
            return sqlite3.connect(str(db_path))

    @staticmethod
    def _rotate_invalid_database(db_path: Path) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        backup_path = db_path.with_name(f"{db_path.name}.invalid-{timestamp}")
        db_path.rename(backup_path)
        return backup_path

    def _ensure_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_metadata (
                dataset_id TEXT PRIMARY KEY,
                query TEXT,
                title_study TEXT,
                metadata_json_path TEXT,
                metadata_json TEXT,
                metadata_xml_path TEXT,
                metadata_xml TEXT,
                resources_json TEXT,
                data_files_json TEXT,
                pdf_paths_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self._conn.commit()

    def upsert_dataset(
        self,
        dataset: dict[str, Any],
        download_result: dict[str, Any],
        query: str,
    ) -> None:
        """Insert or update one dataset row with JSON/XML metadata and path columns."""
        dataset_id = str(download_result.get("dataset_id") or dataset.get("id") or "unknown")

        metadata_json_path = download_result.get("metadata_path")
        resources = download_result.get("resources", []) or []
        data_files = download_result.get("data_files", []) or []

        metadata_xml_path = self._find_xml_metadata_path(resources)
        pdf_paths = self._collect_pdf_paths(resources, data_files)

        metadata_json = self._read_text(metadata_json_path)
        if metadata_json is None:
            metadata_json = json.dumps(dataset, ensure_ascii=False)

        metadata_xml = self._read_text(metadata_xml_path)

        self._conn.execute(
            """
            INSERT INTO dataset_metadata (
                dataset_id,
                query,
                title_study,
                metadata_json_path,
                metadata_json,
                metadata_xml_path,
                metadata_xml,
                resources_json,
                data_files_json,
                pdf_paths_json,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(dataset_id) DO UPDATE SET
                query = excluded.query,
                title_study = excluded.title_study,
                metadata_json_path = excluded.metadata_json_path,
                metadata_json = excluded.metadata_json,
                metadata_xml_path = excluded.metadata_xml_path,
                metadata_xml = excluded.metadata_xml,
                resources_json = excluded.resources_json,
                data_files_json = excluded.data_files_json,
                pdf_paths_json = excluded.pdf_paths_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            [
                dataset_id,
                query,
                dataset.get("titleStudy"),
                metadata_json_path,
                metadata_json,
                metadata_xml_path,
                metadata_xml,
                json.dumps(resources, ensure_ascii=False),
                json.dumps(data_files, ensure_ascii=False),
                json.dumps(pdf_paths, ensure_ascii=False),
            ],
        )
        self._conn.commit()

    def row_count(self) -> int:
        """Return current number of rows in dataset_metadata."""
        value = self._conn.execute("SELECT COUNT(*) FROM dataset_metadata").fetchone()
        return int(value[0]) if value else 0

    def close(self) -> None:
        self._conn.close()

    def _read_text(self, path_value: Optional[str]) -> Optional[str]:
        if not path_value:
            return None
        path = Path(path_value)
        if not path.exists() or not path.is_file():
            return None
        try:
            return path.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            self._logger.debug("Could not read text from %s: %s", path, exc)
            return None

    @staticmethod
    def _find_xml_metadata_path(resources: list[str]) -> Optional[str]:
        for path in resources:
            if str(path).endswith("metadata.xml"):
                return str(path)
        return None

    @staticmethod
    def _collect_pdf_paths(resources: list[str], data_files: list[str]) -> list[str]:
        combined = [str(p) for p in resources] + [str(p) for p in data_files]
        return [p for p in combined if p.lower().endswith(".pdf")]
