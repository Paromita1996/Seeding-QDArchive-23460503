"""FILES table model and persistence helper."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
from urllib.parse import unquote, urlparse

from .enums import normalize_download_result


@dataclass(slots=True)
class ProjectFile:
    project_id: int
    file_name: str
    file_type: str
    status: str

    @staticmethod
    def from_path(
        project_id: int,
        file_path: str,
        status: str = "SUCCEEDED",
    ) -> "ProjectFile":
        return ProjectFile.from_reference(
            project_id=project_id,
            reference=file_path,
            status=status,
        )

    @staticmethod
    def from_reference(
        project_id: int,
        reference: str,
        status: str = "SUCCEEDED",
    ) -> "ProjectFile":
        parsed = urlparse(reference)
        if parsed.scheme and parsed.netloc:
            file_name = Path(unquote(parsed.path)).name or "unknown"
        else:
            file_name = Path(reference).name or "unknown"

        suffix = Path(file_name).suffix.lower().lstrip(".")
        return ProjectFile(
            project_id=project_id,
            file_name=file_name,
            file_type=suffix or "unknown",
            status=normalize_download_result(status),
        )


class FilesTable:
    TABLE_NAME = "files"

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def create_table(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                file_name TEXT NOT NULL,
                file_type TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('SUCCEEDED', 'FAILED')),
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
            )
            """
        )
        self._ensure_status_column()

    def _ensure_status_column(self) -> None:
        existing_columns = {
            str(row[1]).lower()
            for row in self._conn.execute("PRAGMA table_info(files)")
        }
        if "status" not in existing_columns:
            self._conn.execute(
                """
                ALTER TABLE files
                ADD COLUMN status TEXT NOT NULL
                DEFAULT 'SUCCEEDED'
                CHECK(status IN ('SUCCEEDED', 'FAILED'))
                """
            )

    def insert(self, project_file: ProjectFile) -> int:
        cursor = self._conn.execute(
            """
            INSERT INTO files (project_id, file_name, file_type, status)
            VALUES (?, ?, ?, ?)
            """,
            (
                project_file.project_id,
                project_file.file_name,
                project_file.file_type,
                normalize_download_result(project_file.status),
            ),
        )
        return int(cursor.lastrowid)
