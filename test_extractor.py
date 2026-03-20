#!/usr/bin/env python3
"""Quick test script to verify CESSDA API connection."""

import subprocess
import sys

from src.cessda_extractor import CESSDAExtractor


def test_connection():
    """Test basic connection to CESSDA API."""
    print("Testing CESSDA API connection...")
    print("=" * 60)
    
    try:
        extractor = CESSDAExtractor(output_dir="test_output")
        
        # Simple test query
        print("\n1. Testing search functionality...")
        results = extractor.search(query="education", limit=3)
        
        # Check if we got results
        results_count = results.get('ResultsCount', {})
        available = results_count.get('available', 0)
        print("   ✓ API responded successfully")
        print(f"   ✓ Found {available} total results for 'education'")
        
        # Get datasets
        datasets = results.get('Results', [])
        print(f"   ✓ Retrieved {len(datasets)} datasets in first page")
        
        # Display first dataset info
        if datasets:
            print("\n2. Sample dataset information:")
            dataset = datasets[0]
            title = dataset.get('titleStudy', 'N/A')
            print(f"   Title: {title[:80]}...")
            print(f"   ID: {dataset.get('id', 'N/A')}")

            publisher_filter = dataset.get('publisherFilter')
            if publisher_filter and isinstance(publisher_filter, dict):
                print(f"   Publisher: {publisher_filter.get('publisher', 'N/A')}")

        print("\n3. Testing complete download (metadata + resources)...")
        download_results = extractor.query_and_download(
            query="education",
            max_results=2,
            download_resources=True,
            download_files=False,
        )
        print(f"   ✓ Downloaded {len(download_results)} complete datasets")
        for result in download_results:
            n_resources = len(result.get('resources', []))
            n_data_files = len(result.get('data_files', []))
            print(f"   - Dataset {result['dataset_id']}: {n_resources} resource(s), {n_data_files} data file(s)")

        print("\n4. Testing Typer CLI entrypoint...")
        cli_check = subprocess.run(
            [sys.executable, "-m", "src.cessda_extractor", "--help"],
            capture_output=True,
            text=True,
            check=True,
        )
        if "Usage" not in cli_check.stdout:
            raise RuntimeError("Typer CLI help output not detected")
        print("   ✓ Typer CLI responds to --help")

        print("\n" + "=" * 60)
        print("✓ All tests passed! The CESSDA extractor is working correctly.")
        print("\nYou can now use the extractor with your own queries:")
        print("  python -m src.cessda_extractor --help")
        print("  python -m src.cessda_extractor 'your query here'")
        
    except Exception as e:
        print(f"\n✗ Error occurred: {e}")
        print("\nPlease check:")
        print("  1. Internet connection")
        print("  2. CESSDA API availability (https://datacatalogue.cessda.eu)")
        raise


if __name__ == '__main__':
    test_connection()
