import requests
import csv
import json
import os

# Read the dataset IDs from the JSON file
with open("dataset_identifiers.json", "r") as json_file:
    dataset_ids = json.load(json_file)

# Create a folder to store the CSV files
output_folder = "dataset_records"
os.makedirs(output_folder, exist_ok=True)

for dataset_id in dataset_ids:
    # Check if the CSV file already exists
    csv_filename = f"{output_folder}/{dataset_id}.csv"
    if os.path.exists(csv_filename):
        print(f"Skipping dataset {dataset_id} as the CSV file already exists.")
        continue

    # Make an API request to retrieve the dataset metadata
    url = f"https://catalog.data.gov/api/3/action/package_show?id={dataset_id}"
    response = requests.get(url)

    if response.status_code == 200:
        dataset_metadata = response.json()["result"]

        # Extract the URL for the dataset records
        records_url = None
        for resource in dataset_metadata["resources"]:
            if resource["format"].lower() == "csv":
                records_url = resource["url"]
                break

        if records_url:
            # Modify the records URL to retrieve only the first 100 records
            records_url += "&limit=100"

            # Make an API request to retrieve the first 100 records
            response = requests.get(records_url)

            if response.status_code == 200:
                # Parse the CSV data
                csv_data = csv.reader(response.text.strip().split("\n"))
                headers = next(csv_data)

                # Open the CSV file for writing
                with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
                    csv_writer = csv.writer(csvfile)

                    # Write the header row
                    csv_writer.writerow(headers)

                    # Write the retrieved records
                    for row in csv_data:
                        csv_writer.writerow(row)

                print(f"Successfully wrote the first 100 records for dataset {dataset_id} to {csv_filename}")
            else:
                print(f"Failed to retrieve records for dataset {dataset_id}")
        else:
            print(f"CSV resource not found for dataset {dataset_id}")
    else:
        print(f"Failed to retrieve metadata for dataset {dataset_id}")
