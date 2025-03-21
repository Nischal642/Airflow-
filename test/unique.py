import json

# Function to load JSON data from a file
def load_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
        print(f"Type of loaded data: {type(data)}")  # Debug statement
        return data

# Function to save JSON data to a file
def save_json(data, file_path):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

# Function to extract unique contents along with their title and image_ids
def extract_unique_contents(data):
    unique_contents = {}
    for item in data:
        content = item['contents']
        if content not in unique_contents:
            unique_contents[content] = {
                'title': item['title'],
                'image_ids': item['image_ids']
            }
    return unique_contents

# Main function to process the JSON file
def process_json(input_file, output_file):
    # Load the JSON data
    json_data = load_json(input_file)
    
    # Ensure the JSON data has the expected structure
    if not isinstance(json_data, dict) or 'data' not in json_data:
        raise ValueError("The JSON file must contain a 'data' key with a list of objects.")
    
    # Extract the list of items from the 'data' key
    data = json_data['data']
    
    # Ensure data is a list
    if not isinstance(data, list):
        raise ValueError("The 'data' key must contain a list of objects.")
    
    # Extract unique contents
    unique_contents = extract_unique_contents(data)
    
    # Convert the unique contents to a list of dictionaries
    unique_data = [{'title': details['title'], 'contents': content, 'image_ids': details['image_ids']} 
                   for content, details in unique_contents.items()]
    
    # Save the unique data to a new JSON file
    save_json(unique_data, output_file)

# File paths
input_json_file = '/home/nischalacharya/Documents/Pipeline/airflow/Output/Nepal_Profile.json'
output_json_file = '/home/nischalacharya/Documents/Pipeline/airflow/Output/Nepal_2Profile.json'

# Process the JSON file
process_json(input_json_file, output_json_file)