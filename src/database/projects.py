"""PROJECTS table model and persistence helper."""

from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from typing import Optional


@dataclass(slots=True)
class Project:
    repository_id: int
    repository_url: str
    project_url: str
    title: str
    description: str
    download_date: str
    download_repository_folder: str
    download_project_folder: str
    download_method: str
    query_string: Optional[str] = None
    version: Optional[str] = None
    language: Optional[str] = None
    doi: Optional[str] = None
    upload_date: Optional[str] = None
    download_version_folder: Optional[str] = None


class ProjectsTable:
    TABLE_NAME = "projects"

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def create_table(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query_string TEXT,
                repository_id INTEGER NOT NULL,
                repository_url TEXT NOT NULL,
                project_url TEXT NOT NULL,
                version TEXT,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                language TEXT,
                doi TEXT,
                upload_date TEXT,
                download_date TEXT NOT NULL,
                download_repository_folder TEXT NOT NULL,
                download_project_folder TEXT NOT NULL,
                download_version_folder TEXT,
                download_method TEXT NOT NULL CHECK(download_method IN ('SCRAPING', 'API-CALL'))
            )
            """
        )

    def insert(self, project: Project) -> int:
        cursor = self._conn.execute(
            """
            INSERT INTO projects (
                query_string,
                repository_id,
                repository_url,
                project_url,
                version,
                title,
                description,
                language,
                doi,
                upload_date,
                download_date,
                download_repository_folder,
                download_project_folder,
                download_version_folder,
                download_method
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                project.query_string,
                project.repository_id,
                project.repository_url,
                project.project_url,
                project.version,
                project.title,
                project.description,
                project.language,
                project.doi,
                project.upload_date,
                project.download_date,
                project.download_repository_folder,
                project.download_project_folder,
                project.download_version_folder,
                project.download_method,
            ),
        )
        return int(cursor.lastrowid)
