"""CESSDA API client layer."""

from typing import Any, Optional

import requests


class CESSDAClient:
    """Talks to the CESSDA Data Catalogue v2 search API."""

    BASE_URL = "https://datacatalogue.cessda.eu/api/DataSets/v2"
    SEARCH_ENDPOINT = f"{BASE_URL}/search"

    def __init__(self, logger) -> None:
        self._logger = logger
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                ),
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

    def search(
        self,
        query: str = "",
        offset: int = 0,
        limit: int = 10,
        metadata_language: str = "en",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Return one page of search results."""
        params: dict[str, Any] = {
            "metadataLanguage": metadata_language,
            "offset": offset,
            "limit": min(limit, 200),
            **kwargs,
        }
        # Always restrict results to open-access datasets.
        params["dataAccess[]"] = "Open"
        if query:
            params["q"] = query

        self._logger.debug(
            "Searching CESSDA: query='%s', offset=%s, limit=%s",
            query,
            offset,
            limit,
        )
        response = self.session.get(self.SEARCH_ENDPOINT, params=params)
        response.raise_for_status()
        return response.json()

    def get_all_results(
        self,
        query: str = "",
        max_results: Optional[int] = None,
        limit: int = 200,
        metadata_language: str = "en",
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Paginate through search results and return up to max_results records."""
        all_datasets: list[dict[str, Any]] = []
        offset = 0
        limit = min(limit, 200)

        while True:
            page = self.search(
                query=query,
                offset=offset,
                limit=limit,
                metadata_language=metadata_language,
                **kwargs,
            )

            datasets = page.get("Results", [])
            if not datasets:
                break

            all_datasets.extend(datasets)

            counts = page.get("ResultsCount", {})
            available: int = counts.get("available", 0)
            retrieved: int = counts.get("retrieved", 0)
            self._logger.debug("Retrieved %s/%s datasets", len(all_datasets), available)

            if max_results and len(all_datasets) >= max_results:
                all_datasets = all_datasets[:max_results]
                break

            if len(all_datasets) >= available or retrieved < limit:
                break

            offset += limit

        self._logger.debug("Total datasets retrieved: %s", len(all_datasets))
        return all_datasets
