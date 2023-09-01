import os
import sys
from collections import defaultdict
from Utils import save_to_log, save_json_array_to_file
from LabelEmailsUsingDomain import create_origin_label, aggregate_meta_data_from_labeled_data, aggregate_meta_data_from_meta_data, enrich_country_dict

def label_all_files_in_path(path: str, domains_count):
    if not os.path.isdir(path):
        print("bad path")
    for root, directories, files in os.walk(path):
        for file_name in files:
            if not "data" in file_name and not "meta" in file_name and not "log" in file_name:
                file_path = os.path.join(root, file_name)
                label_file(file_path, domains_count)

def label_file(file_path: str, domains_count):
    if os.path.isfile(file_path):
        try:
            data = create_origin_label(file_path, domains_count)
            save_json_array_to_file(data, file_path.replace("\\", "/") + "_labeled_data.json")
        except Exception as e:
            save_to_log(log_path, "Error in file: " + file_path + f"\t{e}")


def calculate_meta_data_by_directory(path: str, domains_count):
    if os.path.isfile(path) and path.find("labeled_data") > 0:
        aggregate_meta_data_from_labeled_data(path.replace("\\", "/"), domains_count)
    elif os.path.isdir(path):
        for root, directories, files in os.walk(path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                calculate_meta_data_by_directory(file_path, domains_count)
        save_json_array_to_file(domains_count, path.replace("\\", "/") + "/dir_meta_data.json")


def aggreagte_meta_data_from_meta_data_files(path: str, domains_count):
    if os.path.isfile(path) and path.find("meta_data") > 0:
        aggregate_meta_data_from_meta_data(path.replace("\\", "/"), domains_count)
    elif os.path.isdir(path):
        for root, directories, files in os.walk(path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                aggreagte_meta_data_from_meta_data_files(file_path, domains_count)
        save_json_array_to_file(domains_count, path.replace("\\", "/") + "/dir_meta_data.json")

def create_country_files(destination_path: str, path: str, country: str, country_data: list, file_index: int):
    total_passwords = 0
    if not os.path.exists(destination_path + "" f"/{country}"):
        os.makedirs(destination_path + "" f"/{country}")
    MAX_FILE_ENTRIES = 50000
    if os.path.isfile(path) and path.find("labeled_data") > 0:
        
        (country_data, file_index) = enrich_country_dict(destination_path, path, country, country_data, MAX_FILE_ENTRIES, file_index)
        total_passwords += len(country_data)
        return (country_data, file_index)
    elif os.path.isdir(path):
        for root, directories, files in os.walk(path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                (country_data, file_index) = create_country_files(destination_path=destination_path, path=file_path, country=country, country_data=country_data, file_index=file_index)
        total_passwords += len(country_data)
        return (country_data, file_index)
    with open(destination_path + "" f"/{country}/meta_data.json", 'w+') as f:
        f.write(f"Total passwords: {total_passwords}\n")
    return (country_data, file_index)
    
    
def main():
    path = sys.argv[1]
    global log_path
    function = sys.argv[2]
    log_path = os.path.join(path, f"{function}_log.txt")
    with open(log_path, 'w+') as f:
        f.write(f"Start {function}\n")
    domains_count = defaultdict(int)
    if function == "label":
        label_all_files_in_path(path, domains_count)
        domains_count["total"] = sum(domains_count.values())
        save_json_array_to_file(domains_count, path + "/meta_data.json")
    elif function == "meta_data":
        calculate_meta_data_by_directory(path, domains_count=domains_count)
    elif function == "aggregate":
        aggreagte_meta_data_from_meta_data_files(path, domains_count)
    elif function == "country":
        country = sys.argv[4]
        country_data = []
        destination_path = sys.argv[3].replace("\\", "/")
        create_country_files(destination_path=destination_path, path=path, country=country, country_data=country_data, file_index=0)

if __name__ == "__main__":
    main()

# path = "C:\\Users\\nirfi\\Downloads\\BreachCompilation\\data"
# cont = ["Italy", "Poland", "China", "United Kingdom (common practice)", "France", "Germany", "Japan"]
# for country in cont:
  #      countries = ["Italy", "Poland", "China", "United Kingdom (common practice)", "France", "Germany", "Japan", "India"]
# 
#     json_array = []
#     country_data = []
#     destination_path = "C:\\Users\\nirfi\\Desktop\\data_by_country\\new_data"
#     create_country_files(destination_path=destination_path, path=path, country=country, country_data=country_data, file_index=0)
