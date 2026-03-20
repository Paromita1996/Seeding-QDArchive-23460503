"""LICENSES table model and persistence helper."""

from __future__ import annotations

from dataclasses import dataclass
import sqlite3


@dataclass(slots=True)
class ProjectLicense:
    project_id: int
    license: str


class LicensesTable:
    TABLE_NAME = "licenses"

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def create_table(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS licenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                license TEXT NOT NULL,
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
            )
            """
        )

    def insert(self, project_license: ProjectLicense) -> int:
        cursor = self._conn.execute(
            """
            INSERT INTO licenses (project_id, license)
            VALUES (?, ?)
            """,
            (project_license.project_id, project_license.license.strip()),
        )
        return int(cursor.lastrowid)
