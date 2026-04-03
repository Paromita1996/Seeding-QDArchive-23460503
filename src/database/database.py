"""High-level SQLite database manager for the CSV-defined metadata schema."""

from __future__ import annotations

from datetime import datetime, UTC
from pathlib import Path
import sqlite3
from typing import Any
from urllib.parse import urlparse

from .files import FilesTable, ProjectFile
from .keywords import KeywordsTable, ProjectKeyword
from .licenses import LicensesTable, ProjectLicense
from .person_roles import PersonRolesTable, ProjectPersonRole
from .projects import Project, ProjectsTable


class MetadataDatabase:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.execute("PRAGMA foreign_keys = ON")

        self.projects = ProjectsTable(self.conn)
        self.files = FilesTable(self.conn)
        self.keywords = KeywordsTable(self.conn)
        self.person_roles = PersonRolesTable(self.conn)
        self.licenses = LicensesTable(self.conn)

    def create_schema(self) -> None:
        self.projects.create_table()
        self.files.create_table()
        self.keywords.create_table()
        self.person_roles.create_table()
        self.licenses.create_table()
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def row_count(self) -> int:
        value = self.conn.execute("SELECT COUNT(*) FROM projects").fetchone()
        return int(value[0]) if value else 0

    def __enter__(self) -> "MetadataDatabase":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        self.close()

    def ingest_dataset(
        self,
        dataset: dict[str, Any],
        download_result: dict[str, Any],
        query_string: str,
        repository_id: int = 1,
        download_method: str = "API-CALL",
    ) -> int:
        project = self._build_project_record(
            dataset=dataset,
            download_result=download_result,
            query_string=query_string,
            repository_id=repository_id,
            download_method=download_method,
        )
        project_id = self.projects.insert(project)

        for file_reference, status in self._collect_file_references(download_result):
            self.files.insert(
                ProjectFile.from_reference(
                    project_id=project_id,
                    reference=file_reference,
                    status=status,
                )
            )

        for term in self._extract_keywords(dataset):
            self.keywords.insert(ProjectKeyword(project_id=project_id, keyword=term))

        for person in dataset.get("creators", []) or []:
            name = str(person.get("name") or "Unknown")
            self.person_roles.insert(
                ProjectPersonRole(project_id=project_id, name=name, role="AUTHOR")
            )

        for license_value in self._extract_licenses(dataset):
            self.licenses.insert(
                ProjectLicense(project_id=project_id, license=license_value)
            )

        self.conn.commit()
        return project_id

    def _build_project_record(
        self,
        dataset: dict[str, Any],
        download_result: dict[str, Any],
        query_string: str,
        repository_id: int,
        download_method: str,
    ) -> Project:
        metadata_path = str(download_result.get("metadata_path") or "")
        path_info = self._split_download_folders(metadata_path)

        project_url = str(dataset.get("studyUrl") or dataset.get("studyXmlSourceUrl") or "")
        repository_url = self._repository_url_from_project_url(project_url)

        return Project(
            query_string=query_string,
            repository_id=repository_id,
            repository_url=repository_url,
            project_url=project_url,
            version=str(dataset.get("version") or "") or None,
            title=str(dataset.get("titleStudy") or dataset.get("id") or "Untitled project"),
            description=str(dataset.get("abstract") or ""),
            language=self._extract_language(dataset),
            doi=self._extract_doi(dataset),
            upload_date=self._extract_upload_date(dataset),
            download_date=datetime.now(UTC).replace(microsecond=0).isoformat(),
            download_repository_folder=path_info["repository_folder"],
            download_project_folder=path_info["project_folder"],
            download_version_folder=path_info["version_folder"],
            download_method=download_method,
        )

    @staticmethod
    def _collect_file_references(download_result: dict[str, Any]) -> list[tuple[str, str]]:
        values: list[tuple[str, str]] = []
        metadata_path = download_result.get("metadata_path")
        if metadata_path:
            values.append((str(metadata_path), "SUCCEEDED"))
        for value in download_result.get("resources", []) or []:
            values.append((str(value), "SUCCEEDED"))
        for value in download_result.get("data_files", []) or []:
            values.append((str(value), "SUCCEEDED"))
        for failed in download_result.get("failed_resources", []) or []:
            if isinstance(failed, dict):
                reference = str(failed.get("reference") or failed.get("url") or "").strip()
            else:
                reference = str(failed).strip()
            if reference:
                values.append((reference, "FAILED"))
        return values

    @staticmethod
    def _extract_keywords(dataset: dict[str, Any]) -> list[str]:
        terms: list[str] = []
        for item in dataset.get("keywords", []) or []:
            term = item.get("term") or item.get("id")
            if term:
                terms.append(str(term))
        return terms

    @staticmethod
    def _extract_licenses(dataset: dict[str, Any]) -> list[str]:
        licenses: list[str] = []

        def _append_if_text(value: Any) -> None:
            if isinstance(value, str) and value.strip():
                licenses.append(value.strip())

        direct_license = dataset.get("license")
        if isinstance(direct_license, dict):
            _append_if_text(direct_license.get("name"))
            _append_if_text(direct_license.get("uri"))
        elif isinstance(direct_license, list):
            for item in direct_license:
                _append_if_text(item)
        else:
            _append_if_text(direct_license)

        rights = dataset.get("rights")
        if isinstance(rights, list):
            for item in rights:
                _append_if_text(item)
        else:
            _append_if_text(rights)

        _append_if_text(dataset.get("termsOfUse"))
        _append_if_text(dataset.get("termsOfAccess"))

        for text in dataset.get("dataAccessFreeTexts", []) or []:
            _append_if_text(text)

        # Some sources provide access labels but no explicit license string.
        _append_if_text(dataset.get("dataAccess"))

        if not licenses:
            licenses.append("UNKNOWN")
        return list(dict.fromkeys(licenses))

    @staticmethod
    def _extract_doi(dataset: dict[str, Any]) -> str | None:
        for pid_info in dataset.get("pidStudies", []) or []:
            agency = str(pid_info.get("agency") or "").upper()
            pid = str(pid_info.get("pid") or "").strip()
            if not pid:
                continue
            if agency == "DOI":
                if pid.lower().startswith("http"):
                    return pid
                return f"https://doi.org/{pid}"
        return None

    @staticmethod
    def _extract_upload_date(dataset: dict[str, Any]) -> str | None:
        value = dataset.get("publicationYear")
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _extract_language(dataset: dict[str, Any]) -> str | None:
        values = dataset.get("langAvailableIn") or dataset.get("fileLanguages") or []
        if isinstance(values, list) and values:
            first = values[0]
            if isinstance(first, str) and first.strip():
                return first.strip()
        return None

    @staticmethod
    def _repository_url_from_project_url(project_url: str) -> str:
        if not project_url:
            return ""
        parsed = urlparse(project_url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return ""

    @staticmethod
    def _split_download_folders(metadata_path: str) -> dict[str, str | None]:
        if not metadata_path:
            return {
                "repository_folder": "",
                "project_folder": "",
                "version_folder": None,
            }

        path = Path(metadata_path)
        project_folder = path.parent.name
        repository_folder = path.parent.parent.name if path.parent.parent != path.parent else ""
        return {
            "repository_folder": repository_folder,
            "project_folder": project_folder,
            "version_folder": None,
        }
