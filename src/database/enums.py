"""Enum-like constants for schema constrained values."""

from __future__ import annotations

PERSON_ROLE_VALUES = {"UPLOADER", "AUTHOR", "OWNER", "OTHER", "UNKNOWN"}

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
