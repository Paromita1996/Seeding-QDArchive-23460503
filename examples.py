"""Example usage of the CESSDA Data Extractor."""

from src.cessda_extractor import CESSDAExtractor


def example_basic_search():
    """Example: Basic search and metadata download."""
    print("Example 1: Basic search")
    print("-" * 50)
    
    extractor = CESSDAExtractor(output_dir="example_output")
    
    # Search for studies about "climate change"
    results = extractor.search(query="climate change", size=5)
    
    print(f"Found {results.get('page', {}).get('totalElements', 0)} total results")
    
    # Get the studies
    studies = results.get('_embedded', {}).get('cmmstudy', [])
    for study in studies:
        print(f"- {study.get('titleStudy', 'No title')}")


def example_download_all():
    """Example: Download all results for a query."""
    print("\nExample 2: Download all matching studies")
    print("-" * 50)
    
    extractor = CESSDAExtractor(output_dir="cessda_data")
    
    # Query and download all matching studies (with resources)
    download_results = extractor.query_and_download(
        query="education",
        max_results=10,  # Limit to 10 for the example
        download_resources=True
    )
    
    print(f"\nDownloaded {len(download_results)} studies")
    for result in download_results:
        print(f"Study ID: {result['study_id']}")
        print(f"  Metadata: {result['metadata_path']}")
        print(f"  Resources: {len(result['resources'])} files")


def example_custom_search():
    """Example: Custom search with filters."""
    print("\nExample 3: Custom search with filters")
    print("-" * 50)
    
    extractor = CESSDAExtractor(output_dir="cessda_data")
    
    # Get all results with custom parameters
    studies = extractor.get_all_results(
        query="health",
        max_results=20,
        lang="en"
    )
    
    print(f"Retrieved {len(studies)} studies")
    
    # Download metadata for each study
    for study in studies[:5]:  # Download first 5
        extractor.download_metadata(study, output_subdir="health_studies")


def example_manual_workflow():
    """Example: Manual workflow with fine control."""
    print("\nExample 4: Manual workflow")
    print("-" * 50)
    
    extractor = CESSDAExtractor(output_dir="cessda_data")
    
    # Step 1: Search
    results = extractor.search(query="survey", size=3)
    studies = results.get('_embedded', {}).get('cmmstudy', [])
    
    # Step 2: Process each study individually
    for study in studies:
        study_id = study.get('id')
        title = study.get('titleStudy', 'No title')
        
        print(f"\nProcessing: {title}")
        
        # Download metadata
        metadata_path = extractor.download_metadata(study, output_subdir=f"study_{study_id}")
        print(f"  Saved metadata: {metadata_path}")
        
        # Extract and download resources
        resource_urls = extractor.extract_resources_from_study(study)
        print(f"  Found {len(resource_urls)} resource URLs")
        
        for idx, url in enumerate(resource_urls):
            extractor.download_resource(
                url,
                filename=f"resource_{idx}.html",
                output_subdir=f"study_{study_id}"
            )


if __name__ == '__main__':
    # Run examples
    # Uncomment the examples you want to run:
    
    example_basic_search()
    # example_download_all()
    # example_custom_search()
    # example_manual_workflow()
