import json

def count_titles(json_file):
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # If data is a list of dictionaries
    if isinstance(data, list):
        return sum(1 for item in data if "title" in item)

    # If data is a dictionary with a nested list
    if isinstance(data, dict) and "data" in data:
        return sum(1 for item in data["data"] if "title" in item)

    return 0  # If structure is unknown

json_file = "/home/nischalacharya/Documents/Pipeline/airflow/Output/Nepal_Profile.json"  # Replace with your actual file path
title_count = count_titles(json_file)
print(f"Total number of 'title' occurrences: {title_count}")
