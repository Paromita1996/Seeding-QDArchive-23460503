"""KEYWORDS table model and persistence helper."""

from __future__ import annotations

from dataclasses import dataclass
import sqlite3


@dataclass(slots=True)
class ProjectKeyword:
    project_id: int
    keyword: str


class KeywordsTable:
    TABLE_NAME = "keywords"

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def create_table(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                keyword TEXT NOT NULL,
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
            )
            """
        )

    def insert(self, project_keyword: ProjectKeyword) -> int:
        cursor = self._conn.execute(
            """
            INSERT INTO keywords (project_id, keyword)
            VALUES (?, ?)
            """,
            (project_keyword.project_id, project_keyword.keyword),
        )
        return int(cursor.lastrowid)
