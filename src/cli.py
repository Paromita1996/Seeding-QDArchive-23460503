"""Unified CLI — query CESSDA or QDR with a single entry-point.

Usage:
    python -m src.cli cessda 'love' --max-results 10
    python -m src.cli qdr    'love' --max-results 10
"""

from __future__ import annotations

from typing import Any, Optional

import typer

from .cessda_extractor import CESSDAExtractor
from .qdr_extractor import QDRExtractor

app = typer.Typer(
    name="seeding-qd",
    help="Extract datasets from CESSDA or QDR and store them in a local SQLite database.",
    no_args_is_help=True,
)


# ---------------------------------------------------------------------------
# CESSDA subcommand
# ---------------------------------------------------------------------------

@app.command("cessda")
def cessda_cmd(
    query: str = typer.Argument(..., help="Search query string"),
    output: str = typer.Option("cessda_data", "-o", "--output", help="Output directory"),
    max_results: Optional[int] = typer.Option(None, "-m", "--max-results", help="Maximum number of results"),
    db_path: Optional[str] = typer.Option(None, "--db-path", help="SQLite path (default: <output>/metadata.sqlite)"),
    no_db: bool = typer.Option(False, "--no-db", help="Disable writing metadata rows to SQLite"),
    no_resources: bool = typer.Option(False, "--no-resources", help="Skip downloading metadata resources"),
    no_files: bool = typer.Option(False, "--no-files", help="Skip downloading data files"),
    progress: bool = typer.Option(True, "--progress/--no-progress", help="Show a progress bar while downloading"),
    lang: str = typer.Option("en", "--lang", help="Metadata language code"),
    publishers: Optional[list[str]] = typer.Option(None, "--publishers", help="Filter by publishers"),
    countries: Optional[list[str]] = typer.Option(None, "--countries", help="Filter by study area countries"),
) -> None:
    """Extract datasets from the CESSDA Data Catalogue."""
    extractor = CESSDAExtractor(
        output_dir=output,
        db_path=db_path,
        persist_to_db=not no_db,
    )
    try:
        search_params: dict[str, Any] = {}
        if publishers:
            search_params["publishers"] = publishers
        if countries:
            search_params["studyAreaCountries"] = countries

        results = extractor.query_and_download(
            query=query,
            max_results=max_results,
            download_resources=not no_resources,
            download_files=not no_files,
            show_progress=progress,
            metadata_language=lang,
            **search_params,
        )

        total_resources = sum(len(r.get("resources", [])) for r in results)
        total_data_files = sum(len(r.get("data_files", [])) for r in results)

        typer.echo(f"\n✓ Downloaded {len(results)} CESSDA dataset(s) to {output}")
        typer.echo(f"  - {total_resources} metadata resource(s)")
        typer.echo(f"  - {total_data_files} data file(s)")
        if extractor.db_path:
            typer.echo(f"  - SQLite: {extractor.db_path}")
            typer.echo(f"  - DB rows: {extractor.db_row_count()}")
    finally:
        extractor.close()


# ---------------------------------------------------------------------------
# QDR subcommand
# ---------------------------------------------------------------------------

@app.command("qdr")
def qdr_cmd(
    query: str = typer.Argument(..., help="Search query string"),
    output: str = typer.Option("qdr_data", "-o", "--output", help="Output directory"),
    max_results: Optional[int] = typer.Option(None, "-m", "--max-results", help="Maximum number of results"),
    db_path: Optional[str] = typer.Option(None, "--db-path", help="SQLite path (default: <output>/metadata.sqlite)"),
    no_db: bool = typer.Option(False, "--no-db", help="Disable writing metadata rows to SQLite"),
    no_files: bool = typer.Option(False, "--no-files", help="Skip downloading data files"),
    progress: bool = typer.Option(True, "--progress/--no-progress", help="Show a progress bar while downloading"),
) -> None:
    """Extract datasets from the QDR Qualitative Data Repository."""
    extractor = QDRExtractor(
        output_dir=output,
        db_path=db_path,
        persist_to_db=not no_db,
    )
    try:
        results = extractor.query_and_download(
            query=query,
            max_results=max_results,
            download_files=not no_files,
            show_progress=progress,
        )

        total_resources = sum(len(r.get("resources", [])) for r in results)
        total_data_files = sum(len(r.get("data_files", [])) for r in results)

        typer.echo(f"\n✓ Downloaded {len(results)} QDR dataset(s) to {output}")
        typer.echo(f"  - {total_resources} resource(s)")
        typer.echo(f"  - {total_data_files} data file(s)")
        if extractor.db_path:
            typer.echo(f"  - SQLite: {extractor.db_path}")
            typer.echo(f"  - DB rows: {extractor.db_row_count()}")
    finally:
        extractor.close()


if __name__ == "__main__":
    app()
