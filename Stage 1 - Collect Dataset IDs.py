import requests
import json

# Step 1: Retrieve dataset identifiers from data.gov
url = "https://catalog.data.gov/api/3/action/package_search?q=&rows=300"
response = requests.get(url)
data = response.json()
dataset_identifiers = [dataset["id"] for dataset in data["result"]["results"]]

# Write dataset identifiers to a JSON file
output_file = "dataset_identifiers.json"
with open(output_file, "w") as file:
    json.dump(dataset_identifiers, file, indent=2)

print("Dataset identifiers saved to dataset_identifiers.json")
