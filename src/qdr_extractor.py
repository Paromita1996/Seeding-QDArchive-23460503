"""QDR Data Extractor entrypoint and high-level facade."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

from rich.logging import RichHandler
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn, TimeElapsedColumn

from .database import MetadataDatabase
from .dataset_downloader import DatasetDownloader
from .qdr_client import QDRClient


logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, markup=True)],
)
logger = logging.getLogger(__name__)

class QDRExtractor:
    """High-level facade: search QDR and download results."""

    def __init__(
        self,
        output_dir: str = "qdr_data",
        db_path: Optional[str] = None,
        persist_to_db: bool = True,
    ) -> None:
        self.client = QDRClient(logger=logger)
        self.downloader = DatasetDownloader(output_dir=output_dir, logger=logger)
        self.metadata_db: Optional[MetadataDatabase] = None
        self.db_path: Optional[str] = None
        if persist_to_db:
            resolved_db_path = db_path or str(Path(output_dir) / "metadata.sqlite")
            self.db_path = resolved_db_path
            self.metadata_db = MetadataDatabase(db_path=resolved_db_path)
            self.metadata_db.create_schema()

    # ------------------------------------------------------------------
    # Passthrough helpers
    # ------------------------------------------------------------------

    def search(
        self,
        query: str = "",
        start: int = 0,
        per_page: int = 10,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Passthrough to QDRClient.search()."""
        return self.client.search(query=query, start=start, per_page=per_page, **kwargs)

    def get_all_results(
        self,
        query: str = "",
        max_results: Optional[int] = None,
        per_page: int = 100,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Passthrough to QDRClient.get_all_results()."""
        return self.client.get_all_results(
            query=query, max_results=max_results, per_page=per_page, **kwargs
        )

    # ------------------------------------------------------------------
    # Core workflow
    # ------------------------------------------------------------------

    def query_and_download(
        self,
        query: str = "",
        max_results: Optional[int] = None,
        download_files: bool = True,
        show_progress: bool = True,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Search QDR and download matching datasets. Returns a list of result dicts."""
        datasets = self.client.get_all_results(
            query=query, max_results=max_results, **kwargs
        )

        if not datasets:
            logger.warning("No datasets found for the query")
            return []

        results: list[dict[str, Any]] = []
        if show_progress:
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeElapsedColumn(),
                transient=True,
            ) as progress:
                task_id = progress.add_task("Downloading QDR datasets", total=len(datasets))
                for dataset in datasets:
                    download_result = self._download_qdr_dataset(
                        dataset, download_files=download_files
                    )
                    results.append(download_result)
                    self._persist_dataset(dataset, download_result, query)
                    progress.advance(task_id)
        else:
            for dataset in datasets:
                download_result = self._download_qdr_dataset(
                    dataset, download_files=download_files
                )
                results.append(download_result)
                self._persist_dataset(dataset, download_result, query)

        logger.info("Finished: %s QDR dataset(s) downloaded", len(results))
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _download_qdr_dataset(
        self,
        dataset: dict[str, Any],
        download_files: bool = True,
    ) -> dict[str, Any]:
        """Download metadata JSON, landing page, and data files for one QDR dataset."""
        global_id: str = str(dataset.get("_qdr_global_id") or dataset.get("id") or "unknown")
        safe_id = DatasetDownloader._sanitize_id(global_id)
        subdir = f"dataset_{safe_id}"

        result: dict[str, Any] = {
            "dataset_id": global_id,
            "metadata_path": None,
            "resources": [],
            "data_files": [],
        }

        # 1. Save the normalized metadata as JSON
        try:
            path = self.downloader.download_metadata(dataset, output_subdir=subdir)
            result["metadata_path"] = str(path)
        except Exception as exc:
            logger.error("Could not save metadata for QDR dataset %s: %s", global_id, exc)

        # 2. Download landing page
        landing_url = dataset.get("studyUrl")
        if landing_url:
            path = self.downloader.download_resource(
                landing_url, "landing_page.html", output_subdir=subdir
            )
            if path:
                result["resources"].append(str(path))

        # 3. Download individual data files via the Dataverse Access API
        files: list[dict[str, Any]] = []
        try:
            enrichment = self.client.get_dataset_enrichment(global_id)
            if enrichment.get("license") is not None:
                dataset["license"] = enrichment.get("license")
            if enrichment.get("termsOfUse") is not None:
                dataset["termsOfUse"] = enrichment.get("termsOfUse")
            if enrichment.get("termsOfAccess") is not None:
                dataset["termsOfAccess"] = enrichment.get("termsOfAccess")
            files = list(enrichment.get("files") or [])
        except Exception as exc:
            logger.warning(
                "Could not fetch metadata enrichment for QDR dataset %s: %s",
                global_id,
                exc,
            )

        if download_files:
            try:
                for file_info in files:
                    data_file = file_info.get("dataFile") or {}
                    file_id = data_file.get("id")
                    filename = (
                        file_info.get("label")
                        or data_file.get("filename")
                        or (f"file_{file_id}" if file_id else None)
                    )
                    restricted = file_info.get("restricted", False)

                    if file_id and filename and not restricted:
                        url = f"{QDRClient.BASE_URL}/access/datafile/{file_id}"
                        path = self.downloader.download_resource(
                            url, str(filename), output_subdir=subdir
                        )
                        if path:
                            result["data_files"].append(str(path))
            except Exception as exc:
                logger.warning(
                    "Could not fetch file list for QDR dataset %s: %s", global_id, exc
                )

        return result

    def _persist_dataset(
        self,
        dataset: dict[str, Any],
        download_result: dict[str, Any],
        query: str,
    ) -> None:
        if not self.metadata_db:
            return
        try:
            self.metadata_db.ingest_dataset(
                dataset=dataset,
                download_result=download_result,
                query_string=query,
            )
        except Exception as exc:
            logger.error(
                "Could not persist QDR dataset %s to SQLite: %s",
                download_result.get("dataset_id", "unknown"),
                exc,
            )

    def db_row_count(self) -> int:
        """Return current number of persisted project rows."""
        if not self.metadata_db:
            return 0
        return self.metadata_db.row_count()

    def close(self) -> None:
        if self.metadata_db:
            self.metadata_db.close()
