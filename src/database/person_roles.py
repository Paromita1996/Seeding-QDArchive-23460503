"""PERSON_ROLE table model and persistence helper."""

from __future__ import annotations

from dataclasses import dataclass
import sqlite3

from .enums import normalize_person_role


@dataclass(slots=True)
class ProjectPersonRole:
    project_id: int
    name: str
    role: str


class PersonRolesTable:
    TABLE_NAME = "person_role"

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def create_table(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS person_role (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('UPLOADER', 'AUTHOR', 'OWNER', 'OTHER', 'UNKNOWN')),
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
            )
            """
        )

    def insert(self, person_role: ProjectPersonRole) -> int:
        cursor = self._conn.execute(
            """
            INSERT INTO person_role (project_id, name, role)
            VALUES (?, ?, ?)
            """,
            (
                person_role.project_id,
                person_role.name,
                normalize_person_role(person_role.role),
            ),
        )
        return int(cursor.lastrowid)
