import json

# Define the input and output file paths
input_file_path = '/home/nischalacharya/Documents/Pipeline/airflow/Output/Nepal_Profile.json'
output_file_path = '/home/nischalacharya/Documents/Pipeline/Nepal_Profile_New.json'

# Read the JSON file
with open(input_file_path, 'r', encoding='utf-8') as file:
    data = file.read()

# Replace "\n" with ","
modified_data = data.replace('\\n', ',')

# Write the modified data to a new file
with open(output_file_path, 'w', encoding='utf-8') as file:
    file.write(modified_data)

print(f"Replacement complete. Modified content saved to {output_file_path}.")