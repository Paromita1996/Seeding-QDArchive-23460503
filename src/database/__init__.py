"""SQLite schema package generated from the provided CSV table definitions."""

from .database import MetadataDatabase
from .files import FilesTable, ProjectFile
from .keywords import KeywordsTable, ProjectKeyword
from .licenses import LicensesTable, ProjectLicense
from .person_roles import PersonRolesTable, ProjectPersonRole
from .projects import Project, ProjectsTable

__all__ = [
    "MetadataDatabase",
    "Project",
    "ProjectsTable",
    "ProjectFile",
    "FilesTable",
    "ProjectKeyword",
    "KeywordsTable",
    "ProjectPersonRole",
    "PersonRolesTable",
    "ProjectLicense",
    "LicensesTable",
]
