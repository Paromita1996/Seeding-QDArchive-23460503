"""FILES table model and persistence helper."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3


@dataclass(slots=True)
class ProjectFile:
    project_id: int
    file_name: str
    file_type: str

    @staticmethod
    def from_path(project_id: int, file_path: str) -> "ProjectFile":
        path = Path(file_path)
        suffix = path.suffix.lower().lstrip(".")
        return ProjectFile(project_id=project_id, file_name=path.name, file_type=suffix or "unknown")


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
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
            )
            """
        )

    def insert(self, project_file: ProjectFile) -> int:
        cursor = self._conn.execute(
            """
            INSERT INTO files (project_id, file_name, file_type)
            VALUES (?, ?, ?)
            """,
            (project_file.project_id, project_file.file_name, project_file.file_type),
        )
        return int(cursor.lastrowid)
