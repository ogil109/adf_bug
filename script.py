import os
import json
import datetime
import re

# Folder name containing JSON files to process
FOLDER_NAME = 'pipeline'

# Script and pipeline paths
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
relative_path = os.path.join(parent_dir, FOLDER_NAME)

# Load the list of JSON files to filter
txt_input_path = os.path.join(script_dir, 'PIPELINE_LIST.txt')
with open(txt_input_path, 'r') as f:
    PIPELINE_LIST = [line.strip() for line in f.readlines()]

print(f'Total JSON files to process: {len(PIPELINE_LIST)}')

# Load already processed JSON files
json_output_path = os.path.join(script_dir, 'procesadas.json')
try:
    with open(json_output_path, 'r') as f:
        processed_pipelines_data = json.load(f)
except FileNotFoundError:
    processed_pipelines_data = []

PROCESSED_PIPELINES = [pipeline for item in processed_pipelines_data for pipeline in item['pipeline_names']]
print(f'JSON files already processed: {len(PROCESSED_PIPELINES)}')

files = []

# Extract the unprocessed JSON files
for file in os.listdir(relative_path):
    if file.endswith('.json') and file not in PROCESSED_PIPELINES and file in PIPELINE_LIST:
        # Load the JSON file
        with open(f"{relative_path}/{file}", 'r', encoding='utf-8') as f:
            data = json.load(f)
        # Filter JSON files with Lookup activities using SnowflakeV2Source
            for activity in data['properties']['activities']:
                if activity['type'] == 'Lookup' and activity['typeProperties']['source']['type'] == 'SnowflakeV2Source':
                    files.append(file)
                    break

print(f'JSON files to process (excluding already processed): {len(files)}')

for file in files:
    # Read the JSON file
    with open(f"{relative_path}/{file}", 'r', encoding='utf-8') as f:
        old_data = json.load(f)
    
        # List of modified activities
        mod_act = []
        
        # Extract and replace activities
        for activity in old_data['properties']['activities']:
            if activity['type'] == 'Lookup' and activity['typeProperties']['source']['type'] == 'SnowflakeV2Source':

                # New script activity template
                new_activity = {
                    "name": "",
                    "type": "Script",
                    "dependsOn": [],
                    "policy": {},
                    "userProperties": [],
                    "linkedServiceName": {
                        "referenceName": "SNOWFLAKE_DYNAMIC_BASIC",
                        "type": "LinkedServiceReference",
                        "parameters": {
                            "accountName": "bx74413.west-europe.privatelink",
                            "database": "STAGING",
                            "warehouse": "DATAFACTORY",
                            "userName": "DATAFACTORY",
                            "secretName": "snowflake-datafactory-pw",
                            "role": ""
                        }
                    },
                    "typeProperties": {
                        "scripts": [
                            {
                                "type": "Query",
                                "text": ""
                            }
                        ],
                        "scriptBlockExecutionTimeout": "02:00:00"
                    }
                }

                # Extract properties from the Lookup activity
                old_query = activity['typeProperties']['source']['query']
                old_name = activity['name']
                old_role = activity['typeProperties']['dataset']['parameters']['role']
                old_depends = activity['dependsOn']
                old_user_properties = activity['userProperties']
                old_policy = activity['policy']
        
                # Replace variables in the new activity template
                new_activity['typeProperties']['scripts'][0]['text'] = old_query
                new_activity['name'] = old_name
                new_activity['linkedServiceName']['parameters']['role'] = old_role
                new_activity['dependsOn'] = old_depends
                new_activity['userProperties'] = old_user_properties
                new_activity['policy'] = old_policy

                # Remove the Lookup activity
                old_data['properties']['activities'].remove(activity)

                # Insert the new Script activity
                old_data['properties']['activities'].append(new_activity)

                # Add modified activity to the list
                mod_act.append(old_name)

        # Replace all 'value' references with 'resultSets[0].rows'
        json_text = json.dumps(old_data)

        for old_name in mod_act:
            # Replace for list references
            pattern = fr"activity\('{old_name}'\)\.output\.value(?!\[)"
            replace = f"if(empty(activity('{old_name}').output.resultSets), activity('{old_name}').output.resultSets, activity('{old_name}').output.resultSets[0].rows)"
            json_text = re.sub(pattern, replace, json_text)
            
            # Replace for specific output value elements
            pattern = f"activity('{old_name}').output.value["
            replace = f"activity('{old_name}').output.resultSets[0].rows["
            json_text = json_text.replace(pattern, replace)

            # Replace for output firstRow
            pattern = f"activity('{old_name}').output.firstRow"
            replace = f"activity('{old_name}').output.resultSets[0].rows[0]"
            json_text = json_text.replace(pattern, replace)

    # Write updated JSON back to file
    with open(f"{relative_path}/{file}", 'w') as f:
        processed_json = json.loads(json_text)
        json.dump(processed_json, f, indent=4)

# Update processed files in procesadas.json
processed_pipelines_data.append({
    'processed_date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    'pipeline_names': files
})

with open(json_output_path, 'w') as f:
    json.dump(processed_pipelines_data, f, indent=4)
