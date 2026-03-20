"""QDR (Qualitative Data Repository) Dataverse API client."""

from __future__ import annotations

from typing import Any, Optional

import requests


class QDRClient:
    """Talks to the QDR Dataverse v1 REST API at data.qdr.syr.edu."""

    BASE_URL = "https://data.qdr.syr.edu/api"
    SEARCH_ENDPOINT = f"{BASE_URL}/search"

    def __init__(self, logger) -> None:
        self._logger = logger
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
            }
        )

    # ------------------------------------------------------------------
    # Core API calls
    # ------------------------------------------------------------------

    def search(
        self,
        query: str = "",
        start: int = 0,
        per_page: int = 10,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Return one page of dataset search results (raw Dataverse response)."""
        params: dict[str, Any] = {
            "q": query or "*",
            "start": start,
            "per_page": min(per_page, 100),
            "type": "dataset",
            **kwargs,
        }
        self._logger.debug(
            "Searching QDR: query='%s', start=%s, per_page=%s", query, start, per_page
        )
        response = self.session.get(self.SEARCH_ENDPOINT, params=params)
        response.raise_for_status()
        return response.json()

    def get_dataset_metadata(self, persistent_id: str) -> dict[str, Any]:
        """Fetch full metadata for a single dataset by its persistent ID (DOI)."""
        response = self.session.get(
            f"{self.BASE_URL}/datasets/:persistentId/",
            params={"persistentId": persistent_id},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def get_dataset_files(self, persistent_id: str) -> list[dict[str, Any]]:
        """Return the file list from the latest published version of a dataset."""
        metadata = self.get_dataset_metadata(persistent_id)
        latest = (metadata.get("data") or {}).get("latestVersion") or {}
        return list(latest.get("files") or [])

    def get_dataset_enrichment(self, persistent_id: str) -> dict[str, Any]:
        """Return extra fields (license/terms/files) from latest dataset version."""
        metadata = self.get_dataset_metadata(persistent_id)
        latest = (metadata.get("data") or {}).get("latestVersion") or {}
        return {
            "license": latest.get("license"),
            "termsOfUse": latest.get("termsOfUse"),
            "termsOfAccess": latest.get("termsOfAccess"),
            "files": list(latest.get("files") or []),
        }

    # ------------------------------------------------------------------
    # Bulk retrieval
    # ------------------------------------------------------------------

    def get_all_results(
        self,
        query: str = "",
        max_results: Optional[int] = None,
        per_page: int = 100,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Paginate through search results and return normalized dataset dicts."""
        all_datasets: list[dict[str, Any]] = []
        start = 0

        while True:
            page = self.search(query=query, start=start, per_page=per_page, **kwargs)
            data = page.get("data") or {}
            items: list[dict[str, Any]] = data.get("items") or []

            if not items:
                break

            for item in items:
                all_datasets.append(self._normalize_dataset(item))

            total_count: int = int(data.get("total_count") or 0)
            self._logger.debug(
                "Retrieved %s/%s QDR datasets", len(all_datasets), total_count
            )

            if max_results and len(all_datasets) >= max_results:
                all_datasets = all_datasets[:max_results]
                break

            if len(all_datasets) >= total_count or len(items) < per_page:
                break

            start += per_page

        self._logger.debug("Total QDR datasets retrieved: %s", len(all_datasets))
        return all_datasets

    # ------------------------------------------------------------------
    # Normalization — produces the standard field names consumed by
    # DatasetDownloader and MetadataDatabase so nothing downstream changes.
    # ------------------------------------------------------------------

    def _normalize_dataset(self, item: dict[str, Any]) -> dict[str, Any]:
        global_id: str = str(item.get("global_id") or "")
        doi_pid = global_id.removeprefix("doi:").removeprefix("DOI:")

        return {
            # Fields expected by DatasetDownloader
            "id": global_id,          # sanitized by downloader for paths
            "titleStudy": str(item.get("name") or ""),
            "studyUrl": str(item.get("url") or ""),
            # Fields expected by MetadataDatabase
            "abstract": str(item.get("description") or ""),
            "publicationYear": str(
                item.get("published_at") or item.get("createdAt") or ""
            ),
            "keywords": [{"term": s} for s in (item.get("subjects") or [])],
            "creators": [{"name": a} for a in (item.get("authors") or [])],
            "langAvailableIn": [],
            "pidStudies": (
                [{"agency": "DOI", "pid": doi_pid}] if doi_pid else []
            ),
            "dataAccess": "Open",
            # QDR-internal fields kept for file downloading
            "_qdr_global_id": global_id,
        }
