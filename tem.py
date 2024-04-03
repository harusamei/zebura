import json

# Specify the path to your JSON file
file_path = '/path/to/your/file.json'

# Open the JSON file and load its contents into a dictionary
with open(file_path, 'r') as file:
    data = json.load(file)

# Now you can access the data as a dictionary
print(data)