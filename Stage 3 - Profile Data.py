import json
from dataprofiler import Data, Profiler
import pandas as pd
import os
import requests
import tempfile

# Helper functions for printing results
def get_structured_results(dataset_id, dataset_name, results):
    """Helper function to get data labels for each column."""
    columns = []
    predictions = []
    samples = []
    for col in results['data_stats']:
        columns.append(col['column_name'])
        predictions.append(col['data_label'])
        samples.append(col['samples'])
    df_results = pd.DataFrame({
        'Dataset ID': dataset_id,
        'Dataset Name': dataset_name,
        'Column': columns,
        'Prediction': predictions,
        'Sample': samples
    })
    return df_results

def get_unstructured_results(data, results):
    """Helper function to get data labels for each labeled piece of text."""
    labeled_data = []
    for pred in results['pred'][0]:
        labeled_data.append([data[0][pred[0]:pred[1]], pred[2]])
    label_df = pd.DataFrame(labeled_data, columns=['Text', 'Labels'])
    return label_df

pd.set_option('display.width', 100)

# Load dataset identifiers from JSON file
with open("./dataset_identifiers.json", "r") as file:
    dataset_identifiers = json.load(file)

# Output file
output_file = "analysis_output.csv"

processed_datasets = []

for dataset_id in dataset_identifiers:
    if dataset_id in processed_datasets:
        print(f"Skipping dataset ID: {dataset_id} (already processed)")
        continue

    try:
        # Get dataset details from API
        dataset_url = f"https://catalog.data.gov/api/3/action/package_show?id={dataset_id}"
        dataset_response = requests.get(dataset_url)
        
        # Check the status code of the response
        if dataset_response.status_code != 200:
            print(f"Error fetching dataset ID: {dataset_id}")
            print(f"Status code: {dataset_response.status_code}")
            print(f"Response text: {dataset_response.text}")
            continue
        
        dataset_data = dataset_response.json()
        
        # Check if the 'result' key exists in the JSON response
        if 'result' not in dataset_data:
            print(f"Error: 'result' key not found in the JSON response for dataset ID: {dataset_id}")
            print(f"Response: {dataset_data}")
            continue
        
        dataset_name = dataset_data["result"]["title"]
        resources = dataset_data["result"].get("resources", [])        
        csv_processed = False
        for resource in resources:
            if resource.get("format", "").lower() == "csv":
                csv_file = f"./dataset_records/{dataset_id}.csv"
                if os.path.exists(csv_file):
                    # Load file (CSV should be automatically identified)
                    df = pd.read_csv(csv_file)
                    # Take the first 100 records
                    df = df.head(100)
                    
                    # Save the first 100 records to a temporary CSV file
                    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                        df.to_csv(temp_file.name, index=False)
                        temp_file_path = temp_file.name

                    # Create a Data object from the temporary CSV file
                    data = Data(temp_file_path)
                    # Profile the dataset
                    profile = Profiler(data)
                    # Generate a report and use json to prettify.
                    report = profile.report(report_options={"output_format": "compact"})
                    # Print the report
                    print('\nLabel Predictions for Dataset ID:', dataset_id)
                    print('=' * 85)
                    print(get_structured_results(dataset_id, dataset_name, report))
                    # Append the results to metadata_output.csv
                    df_results = get_structured_results(dataset_id, dataset_name, report)
                    if not os.path.exists(output_file):
                        df_results.to_csv(output_file, index=False)
                    else:
                        df_results.to_csv(output_file, mode='a', header=False, index=False)
                    
                    # Remove the temporary file
                    os.unlink(temp_file_path)
                    
                    csv_processed = True
                    break

        if not csv_processed:
            print(f"No CSV resource found for dataset ID: {dataset_id}")

        processed_datasets.append(dataset_id)

    except Exception as e:
        print(f"Error processing dataset ID: {dataset_id}")
        print(f"Error details: {str(e)}")
        continue

print("Metadata extraction completed. Results saved to", output_file)
