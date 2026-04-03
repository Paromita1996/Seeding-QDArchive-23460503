"""Dataset download and persistence layer."""

import json
import re
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlsplit

import requests


class DatasetDownloader:
    """Saves dataset metadata and resource files to disk."""

    _KNOWN_EXTENSIONS = (".pdf", ".json", ".xml", ".csv", ".zip")
    _FILE_LINK_PATTERN = re.compile(
        r'https?://[^\s"\'<>]+\.(?:pdf|json|csv|xlsx|zip|xml)',
        re.IGNORECASE,
    )

    def __init__(self, output_dir: str, logger) -> None:
        self._logger = logger
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                ),
            }
        )

    @staticmethod
    def _sanitize_id(value: str) -> str:
        """Return a filesystem-safe version of an arbitrary dataset id."""
        return "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in str(value))

    def download_dataset(
        self,
        dataset: dict[str, Any],
        download_resources: bool = True,
        download_files: bool = True,
    ) -> dict[str, Any]:
        """Download metadata and optional resources for one dataset."""
        dataset_id = dataset.get("id", "unknown")
        subdir = f"dataset_{self._sanitize_id(dataset_id)}"

        result: dict[str, Any] = {
            "dataset_id": dataset_id,
            "metadata_path": None,
            "resources": [],
            "data_files": [],
            "failed_resources": [],
        }

        try:
            path = self.download_metadata(dataset, output_subdir=subdir)
            result["metadata_path"] = str(path)
        except Exception as exc:
            self._logger.error("Could not save metadata for dataset %s: %s", dataset_id, exc)
            result["failed_resources"].append(
                {
                    "reference": self._metadata_filename(dataset),
                    "url": "",
                }
            )

        if download_resources:
            for resource in self._extract_resources(dataset):
                self._process_resource(resource, subdir, download_files, result)

        return result

    def download_metadata(
        self,
        dataset: dict[str, Any],
        output_subdir: Optional[str] = None,
    ) -> Path:
        """Serialize a dataset record to JSON and return the file path."""
        path = self._resolve_path(self._metadata_filename(dataset), output_subdir)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(dataset, fh, indent=2, ensure_ascii=False)
        self._logger.debug("Saved metadata -> %s", path)
        return path

    def _metadata_filename(self, dataset: dict[str, Any]) -> str:
        dataset_id = self._sanitize_id(dataset.get("id", "unknown"))
        raw_title = str(dataset.get("titleStudy", "untitled"))[:50]
        safe_title = "".join(
            c if c.isalnum() or c in (" ", "-", "_") else "_" for c in raw_title
        )
        return f"{dataset_id}_{safe_title}.json"

    def download_resource(
        self,
        url: str,
        filename: str,
        output_subdir: Optional[str] = None,
        follow_redirects: bool = True,
    ) -> Optional[Path]:
        """Download one file from url and return the saved path."""
        try:
            path = self._resolve_path(filename, output_subdir)
            response = self.session.get(
                url,
                stream=True,
                allow_redirects=follow_redirects,
                timeout=30,
            )
            response.raise_for_status()

            content_type = response.headers.get("content-type", "")
            if not any(filename.endswith(ext) for ext in self._KNOWN_EXTENSIONS):
                if "pdf" in content_type:
                    path = path.with_suffix(".pdf")
                elif "json" in content_type:
                    path = path.with_suffix(".json")
                elif "xml" in content_type:
                    path = path.with_suffix(".xml")

            with open(path, "wb") as fh:
                for chunk in response.iter_content(chunk_size=8192):
                    fh.write(chunk)

            self._logger.debug("Saved resource -> %s (%s bytes)", path, path.stat().st_size)
            return path
        except requests.RequestException as exc:
            self._logger.error("Failed to download %s: %s", url, exc)
            return None

    def find_downloadable_files(
        self,
        url: str,
        output_subdir: Optional[str] = None,
        failed_resources: Optional[list[dict[str, str]]] = None,
    ) -> list[Path]:
        """Scrape a landing page and download any directly linked data files."""
        downloaded: list[Path] = []
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            file_urls = self._FILE_LINK_PATTERN.findall(response.text)
            self._logger.debug("Found %s file link(s) on %s", len(file_urls), url)

            for file_url in file_urls:
                filename = Path(urlsplit(file_url).path).name or "unknown"
                path = self.download_resource(file_url, filename, output_subdir=output_subdir)
                if path:
                    downloaded.append(path)
                elif failed_resources is not None:
                    failed_resources.append({"reference": filename, "url": file_url})
        except Exception as exc:
            self._logger.debug("Could not parse landing page %s: %s", url, exc)

        return downloaded

    def _resolve_path(self, filename: str, output_subdir: Optional[str]) -> Path:
        base = self.output_dir / output_subdir if output_subdir else self.output_dir
        base.mkdir(parents=True, exist_ok=True)
        return base / filename

    def _extract_resources(self, dataset: dict[str, Any]) -> list[dict[str, str]]:
        resources = []
        if "studyUrl" in dataset:
            resources.append({"url": dataset["studyUrl"], "type": "landing_page"})
        if "studyXmlSourceUrl" in dataset:
            resources.append({"url": dataset["studyXmlSourceUrl"], "type": "xml_metadata"})
        return resources

    def _process_resource(
        self,
        resource: dict[str, str],
        subdir: str,
        download_files: bool,
        result: dict[str, Any],
    ) -> None:
        url = resource["url"]
        rtype = resource["type"]

        if rtype == "xml_metadata":
            path = self.download_resource(url, "metadata.xml", output_subdir=subdir)
            if path:
                result["resources"].append(str(path))
            else:
                result["failed_resources"].append({"reference": "metadata.xml", "url": url})

        elif rtype == "landing_page" and download_files:
            self._logger.debug("Exploring landing page: %s", url)
            for file_path in self.find_downloadable_files(
                url,
                subdir,
                failed_resources=result["failed_resources"],
            ):
                result["data_files"].append(str(file_path))
            path = self.download_resource(url, "landing_page.html", output_subdir=subdir)
            if path:
                result["resources"].append(str(path))
            else:
                result["failed_resources"].append(
                    {"reference": "landing_page.html", "url": url}
                )
