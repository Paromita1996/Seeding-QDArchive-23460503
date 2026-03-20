"""CESSDA API Data Extractor entrypoint and high-level facade."""

import logging
from pathlib import Path
from typing import Any, Optional

from rich.logging import RichHandler
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn, TimeElapsedColumn

from .cessda_client import CESSDAClient
from .database import MetadataDatabase
from .dataset_downloader import DatasetDownloader


logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, markup=True)],
)
logger = logging.getLogger(__name__)

class CESSDAExtractor:
    """High-level facade: search the catalogue and download results."""

    def __init__(
        self,
        output_dir: str = "cessda_data",
        db_path: Optional[str] = None,
        persist_to_db: bool = True,
    ) -> None:
        self.client = CESSDAClient(logger=logger)
        self.downloader = DatasetDownloader(output_dir=output_dir, logger=logger)
        self.metadata_db: Optional[MetadataDatabase] = None
        self.db_path: Optional[str] = None
        if persist_to_db:
            resolved_db_path = db_path or str(Path(output_dir) / "metadata.sqlite")
            self.db_path = resolved_db_path
            self.metadata_db = MetadataDatabase(db_path=resolved_db_path)
            self.metadata_db.create_schema()

    def search(
        self,
        query: str = "",
        offset: int = 0,
        limit: int = 10,
        metadata_language: str = "en",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Backward-compatible passthrough to client.search()."""
        return self.client.search(
            query=query,
            offset=offset,
            limit=limit,
            metadata_language=metadata_language,
            **kwargs,
        )

    def get_all_results(
        self,
        query: str = "",
        max_results: Optional[int] = None,
        limit: int = 200,
        metadata_language: str = "en",
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Backward-compatible passthrough to client.get_all_results()."""
        return self.client.get_all_results(
            query=query,
            max_results=max_results,
            limit=limit,
            metadata_language=metadata_language,
            **kwargs,
        )

    def query_and_download(
        self,
        query: str = "",
        max_results: Optional[int] = None,
        download_resources: bool = True,
        download_files: bool = True,
        metadata_language: str = "en",
        show_progress: bool = True,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Search CESSDA and download matching datasets. Returns a list of result dicts."""
        datasets = self.client.get_all_results(
            query=query,
            max_results=max_results,
            metadata_language=metadata_language,
            **kwargs,
        )

        if not datasets:
            logger.warning("No datasets found for the query")
            return []

        results = []
        if show_progress:
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeElapsedColumn(),
                transient=True,
            ) as progress:
                task_id = progress.add_task("Downloading datasets", total=len(datasets))
                for dataset in datasets:
                    download_result = self.downloader.download_dataset(
                        dataset,
                        download_resources=download_resources,
                        download_files=download_files,
                    )
                    results.append(download_result)
                    self._persist_dataset(dataset, download_result, query)
                    progress.advance(task_id)
        else:
            for dataset in datasets:
                download_result = self.downloader.download_dataset(
                    dataset,
                    download_resources=download_resources,
                    download_files=download_files,
                )
                results.append(download_result)
                self._persist_dataset(dataset, download_result, query)

        logger.info("Finished: %s dataset(s) downloaded", len(results))
        return results

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
                "Could not persist dataset %s to SQLite: %s",
                download_result.get("dataset_id", "unknown"),
                exc,
            )

    def db_row_count(self) -> int:
        """Return current number of persisted metadata rows."""
        if not self.metadata_db:
            return 0
        return self.metadata_db.row_count()

    def close(self) -> None:
        """Close open resources owned by the extractor."""
        if self.metadata_db:
            self.metadata_db.close()
