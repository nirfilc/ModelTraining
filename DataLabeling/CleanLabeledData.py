import os
import json
import sys

def is_ascii(s):
    return all(ord(char) < 128 for char in s)

def filter_passwords(data):
    filtered_data = []
    ilegal_passwords_count = 0
    for item in data:
        json_item = json.loads(item)
        if 'password' in json_item and is_ascii(json_item['password']):
            filtered_data.append(json_item)
        else:
            ilegal_passwords_count += 1
    return filtered_data, ilegal_passwords_count

def clean_labeled_file(file_name: str, path: str):
    if not str.endswith(file_name, "labeled_data.json"):
        return 0, 0
    file_path = os.path.join(path, file_name)
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            
        filtered_data, file_ilegal_data_count = filter_passwords(data)
        
        with open(file_path, 'w') as file:
            json.dump(filtered_data, file, indent=4)
        
        print("Filtered and updated data saved successfully.")
        return len(filtered_data), file_ilegal_data_count
        
    except FileNotFoundError:
        print("File not found.")
    except json.JSONDecodeError:
        print("Invalid JSON format in the file.")
    except Exception as e:
        print(f"An error occurred: {e}")

def clean_labeled_data(path: str):
    total_legal_passwords, total_ilegal_passwords = 0, 0
    failed_files = []
    if os.path.isdir(path):
        for root, directories, files in os.walk(path):
            # if path has been cleaned and has a cleaned_data.txt file, skip it.
            if "cleaned_data.txt" in files:
               return
            for file_name in files:
                try:
                    legal_passwords, ilegal_passwords = clean_labeled_file(file_name, root)
                    total_legal_passwords += legal_passwords
                    total_ilegal_passwords += ilegal_passwords
                except Exception as e:
                    print(f"Failed to clean file {file_name}. Error: {e}")
                    failed_files.append(file_name)
    with (open(os.path.join(path, "cleaned_data.txt"), "w")) as file:
        file.write(f"Total legal passwords: {total_legal_passwords}\n")
        file.write(f"Total ilegal passwords: {total_ilegal_passwords}\n")
        file.write(f"Total passwords: {total_legal_passwords + total_ilegal_passwords}\n")
        file.write(f"Failed files: {failed_files}\n")




def main():
    base_path = sys.argv[1].replace("\\", "/")
    for root, directories, files in os.walk(base_path):
        if len(directories) == 0:
            clean_labeled_data(base_path)
        else:
            for directory in directories:
                path = os.path.join(base_path, directory)
                clean_labeled_data(path)
        
if __name__ == "__main__":
    main()        

