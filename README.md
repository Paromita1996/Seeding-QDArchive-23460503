<<<<<<< HEAD
## Repository for Paromita Das to work on Master Project "Seeding QDArchive".
Student ID: 23460503
=======
# Seeding QD Archive - CESSDA Data Extractor

A Python tool for querying the CESSDA (Consortium of European Social Science Data Archives) Data Catalogue API and downloading study metadata and resources.

## Features

- **Search CESSDA Catalogue**: Query the CESSDA Data Catalogue with custom search terms
- **Download Metadata**: Save complete study metadata in JSON format
- **Download Resources**: Retrieve associated study resources and data files
- **Download PDFs via DOI**: Automatically download research papers using PyPaperBot with DOI identifiers
- **Download Data Files**: Automatically extract and download PDFs, CSVs, Excel files, and other data files from repository landing pages
- **Repository Support**: Smart detection and download from Dataverse, DANS, and other common repositories
- **Pagination Support**: Automatically handle pagination to retrieve all results
- **Flexible API**: Use as a library or command-line tool

## Installation

```bash
# Clone the repository
cd seeding-qd-archive

# Create and activate virtual environment (if not already done)
python -m venv .venv
source .venv/bin/activate  # On Linux/Mac
# or
.venv\Scripts\activate  # On Windows

# Install dependencies (including PyPaperBot for DOI-based PDF downloads)
pip install -e .
```

## Requirements

- Python 3.7+
- `requests` - HTTP client for API calls
- `scidownl` - For downloading research papers via DOI (optional but recommended)

## Quick Start

### Command Line Usage

```bash
# Basic search and download (includes PDFs and data files)
python -m src.cessda_extractor "climate change"

# Limit number of results
python -m src.cessda_extractor "education" --max-results 10

# Specify output directory
python -m src.cessda_extractor "health survey" --output my_data

# Search in different language
python -m src.cessda_extractor "Bildung" --lang de

# Download only metadata (skip data files)
python -m src.cessda_extractor "migration" --no-files

# Skip downloading PDFs via DOI (only scrape files from landing pages)
python -m src.cessda_extractor "education" --no-pdfs

# Skip downloading metadata resources (only get JSON metadata)
python -m src.cessda_extractor "survey" --no-resources

# Filter by publisher
python -m src.cessda_extractor "education" --publishers "UK Data Service" "GESIS"
```

### Python API Usage

```python
from src.cessda_extractor import CESSDAExtractor

# Initialize extractor
extractor = CESSDAExtractor(output_dir="cessda_data")

# Simple search
results = extractor.search(query="climate change", limit=10)

# Download all matching studies with data files
download_results = extractor.query_and_download(
    query="education",
    max_results=20,
    download_resources=True,   # Download XML metadata and landing pages
    download_files=True,        # Download PDFs, CSVs, and other data files
    download_pdfs_with_doi=True, # Use PyPaperBot to download PDFs via DOI
    metadata_language="en"
)

# Get all results with pagination
studies = extractor.get_all_results(
    query="health",
    max_results=50
)

# Download individual study
for study in studies[:5]:
    extractor.download_metadata(study)
```

## DOI-Based PDF Downloads

The extractor automatically extracts DOI (Digital Object Identifier) information from dataset metadata and uses Scidownl to download associated research papers as PDFs via Sci-Hub.

### How It Works

1. **DOI Extraction**: The extractor looks for DOIs in the dataset's `pidStudies` field and `studyUrl`
2. **Scidownl Integration**: Uses the scidownl library to download PDFs: `scihub_download(paper_url, paper_type="doi", out=output_file)`
3. **Automatic Download**: PDFs are downloaded to the dataset's output directory with safe filenames

### Example

```python
from src.cessda_extractor import CESSDAExtractor

extractor = CESSDAExtractor(output_dir="my_data")

# This will download PDFs for studies that have DOIs
results = extractor.query_and_download(
    query="climate change",
    max_results=10,
    download_pdfs_with_doi=True  # Enable DOI-based PDF downloads (default: True)
)

# Check results
for result in results:
    print(f"Dataset: {result['dataset_id']}")
    print(f"  PDFs downloaded: {len(result.get('pdfs', []))}")
```

### DOI Extraction API

```python
# Extract DOIs from a dataset
dataset = {
    'studyUrl': 'https://doi.org/10.17026/SS/RJVDGG',
    'pidStudies': [
        {'agency': 'DOI', 'pid': 'doi:10.17026/SS/RJVDGG'}
    ]
}

dois = extractor.extract_dois_from_dataset(dataset)
print(dois)  # ['10.17026/SS/RJVDGG']

# Download PDF for a specific DOI
from pathlib import Path
success = extractor.download_pdf_with_scidownl(
    doi='10.17026/SS/RJVDGG',
    output_dir=Path('output')
)
```

### Using Proxies

If you need to use a proxy (e.g., for accessing Sci-Hub), you can pass proxy settings:

```python
# In the extractor code, modify download_pdf_with_scidownl call to include proxies:
proxies = {
    'http': 'socks5://127.0.0.1:7890',
    'https': 'socks5://127.0.0.1:7890'
}

success = extractor.download_pdf_with_scidownl(
    doi='10.17026/SS/RJVDGG',
    output_dir=Path('output'),
    proxies=proxies
)
```

### Disabling DOI-Based Downloads

```bash
# Command line: skip DOI-based PDF downloads
python -m src.cessda_extractor "education" --no-pdfs

# Python API: disable DOI-based downloads
results = extractor.query_and_download(
    query="education",
    download_pdfs_with_doi=False
)
```

## Supported Features

### PDF Download Methods
- **Scidownl**: Via Sci-Hub (primary method) - Downloads papers by DOI
- **Direct repository downloads**: From Dataverse and DANS repositories

### Supported File Types
- PDF - Research papers
- XML - DDI metadata
- JSON - Dataset metadata
- CSV, Excel, ZIP - Data files
- DOCX - Documents

## API Reference

### CESSDAExtractor Class

#### `__init__(output_dir: str = "cessda_data")`
Initialize the extractor with an output directory for downloaded data.

#### `search(query: str = "", offset: int = 0, limit: int = 10, metadata_language: str = "en", **kwargs)`
Search the CESSDA Data Catalogue using v2 API.
- **query**: Search query string (optional)
- **offset**: Number of items to skip for pagination
- **limit**: Number of results to return, max 200
- **metadata_language**: Language code (en, de, fr, nl, fi, sv, etc.) - required
- Returns: Dictionary with search results including ResultsCount and Results

#### `get_all_results(query: str = "", max_results: Optional[int] = None, limit: int = 200, metadata_language: str = "en", **kwargs)`
Retrieve all search results across multiple pages.
- **query**: Search query string (optional)
- **max_results**: Maximum number of results to retrieve (None for all)
- **limit**: Number of results per request, max 200
- **metadata_language**: Language code
- Returns: List of all dataset records

#### `download_metadata(dataset: Dict[str, Any], output_subdir: Optional[str] = None)`
Download and save metadata for a single dataset.
- **dataset**: Dataset record from search results
- **output_subdir**: Optional subdirectory within output_dir
- Returns: Path to the saved metadata file

#### `find_downloadable_files(landing_page_url: str, output_subdir: str)`
Attempt to find and download files (PDFs, CSVs, etc.) from a dataset landing page.
- **landing_page_url**: URL of the dataset landing page (often a DOI)
- **output_subdir**: Directory to save downloaded files
- Returns: List of paths to successfully downloaded files

#### `query_and_download(query: str = "", max_results: Optional[int] = None, download_resources: bool = True, download_files: bool = True, metadata_language: str = "en", **search_kwargs)`
Query CESSDA API and download all matching datasets with their data files.
- **query**: Search query string (optional)
- **max_results**: Maximum number of results to download
- **download_resources**: Whether to download metadata resources (XML, landing pages)
- **download_files**: Whether to download actual data files (PDFs, CSVs, Excel, etc.)
- **metadata_language**: Language code
- Returns: List of download results for each dataset

## Examples

See [examples.py](examples.py) for more detailed usage examples.

## Output Structure

Downloaded data is organized as follows:

```
cessda_data/
├── dataset_<id1>/
│   ├── <id1>_<title>.json       # Dataset metadata (JSON)
│   ├── metadata.xml              # DDI metadata (XML)
│   ├── landing_page.html         # Repository landing page
│   ├── datafile_614657           # Actual data file (CSV/TSV)
│   ├── datafile_614712.xml       # Document file (DOCX/PDF)
│   └── ...                       # Additional data files
├── dataset_<id2>/
│   ├── <id2>_<title>.json
│   ├── metadata.xml
│   ├── supplementary.pdf         # Research paper/supplementary materials
│   └── ...
└── ...
```

### Downloaded File Types

The extractor can download various file types from repositories:
- **Metadata**: JSON, XML (DDI format)
- **Data Files**: CSV, TSV, Excel (XLSX), SPSS (SAV)
- **Documents**: PDF, Word (DOCX), Text files
- **Compressed**: ZIP archives
- **Web Pages**: HTML landing pages for reference

## CESSDA Data Catalogue

The CESSDA Data Catalogue provides access to metadata for thousands of social science datasets from European data archives. Learn more at: https://datacatalogue.cessda.eu/

## License

This project is open source. Please check the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
>>>>>>> 1ad3763 (initilize the project with ceesda and qdr datasets)
