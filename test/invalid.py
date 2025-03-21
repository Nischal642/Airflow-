import json
import re

def clean_json_file(file_path, output_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            raw_content = f.read()

        # Fix invalid escape sequences
        fixed_content = re.sub(r'\\(?!["\\/bfnrtu])', r'\\\\', raw_content)

        # Parse JSON
        data = json.loads(fixed_content)

        # Save the fixed JSON
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        print(f"Fixed JSON saved to: {output_path}")
    
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

# Example usage
input_file = "/home/nischalacharya/Documents/Pipeline/airflow/Output/Nepal_Profile.json"  # Replace with your actual JSON file path
output_file = "/home/nischalacharya/Documents/Pipeline/airflow/Output/Nepal_Profile3.json"   # Output file
clean_json_file(input_file, output_file)
