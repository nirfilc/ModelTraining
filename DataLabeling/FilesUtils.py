import json

def save_json_array_to_file(data, file_path):
    with open(file_path, 'w+') as file:
        json.dump(data, file, indent=4)

def save_to_log(log_path, data):
    with open(log_path, 'a+') as file:
        file.write(data + "\n")
