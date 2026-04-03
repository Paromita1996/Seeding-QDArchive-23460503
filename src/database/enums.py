"""Enum-like constants for schema constrained values."""

from __future__ import annotations

PERSON_ROLE_VALUES = {"UPLOADER", "AUTHOR", "OWNER", "OTHER", "UNKNOWN"}
DOWNLOAD_RESULT_VALUES = {"SUCCEEDED", "FAILED"}

# Keep this permissive. If raw data contains another license string, store it as-is.
KNOWN_LICENSE_PREFIXES = {
    "CC BY",
    "CC BY-SA",
    "CC BY-NC",
    "CC BY-ND",
    "CC BY-NC-ND",
    "CC0",
    "ODBL",
    "ODBL-1.0",
    "ODC-BY",
    "ODC-BY-1.0",
    "PDDL",
}


def normalize_person_role(role: str | None) -> str:
    if not role:
        return "UNKNOWN"
    normalized = role.strip().upper()
    return normalized if normalized in PERSON_ROLE_VALUES else "UNKNOWN"


def normalize_download_result(status: str | None) -> str:
    if not status:
        return "FAILED"
    normalized = status.strip().upper()
    return normalized if normalized in DOWNLOAD_RESULT_VALUES else "FAILED"
