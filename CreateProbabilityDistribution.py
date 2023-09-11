import datetime
import heapq
import os
from collections import defaultdict
import sys
import ModelTrainingUtils
import json
from pathlib import Path
import multiprocessing
import DataLabelingUtils

def save_distribution(data: dict, file_name: str, path: str):
    """
        Saves the dictionary in the provided path with the provided name

        Args:
            data: A dictionary of {key: p} pairs where Sum(dict.keys()) == 1
            file_name: The desiered name to the file created
            path: The path to the file created
    """
    distribution_path = os.path.join(path, "distributions")
    Path(distribution_path).mkdir(parents=True, exist_ok=True)
    file_name = os.path.join(distribution_path, file_name + ".txt")
    sorted_data = dict(sorted(data.items()))

    with open(file_name, "w") as file :
        for key,value in sorted_data.items():
            file.write(f"{repr(key)[1:-1]} {value}\n")

def enrich_counts_from_files(file_path: str, base_word_count: defaultdict, prefix_count: defaultdict, suffix_count: defaultdict, shift_pattern_count: defaultdict, leet_pattern_count: defaultdict):
    """
        Enriches the provided dictionaries according to the passowrds in the provided path
    """
    total_passwords, ilegal_passwords = 0, 0
    with open(file_path, "r") as read_file:
        array = json.load(read_file)
        for user in array:
            password = str(user['password']).removesuffix("\n")
            if (not DataLabelingUtils.is_legal_password(password)) or (DataLabelingUtils.is_short_and_not_date(password)):
                ilegal_passwords += 1
                continue
            [prefix, base_word, suffix] = ModelTrainingUtils.parse_password_to_3d(password)
            if suffix_count != None:
                suffix_count[suffix] += 1
            if prefix_count != None:
                prefix_count[prefix] += 1
            if shift_pattern_count != None:
                shift_pattern = ModelTrainingUtils.get_base_word_shift_pattern(base_word)
                if len(shift_pattern) == len(base_word):
                    shift_pattern_as_string = "all-cap"
                shift_pattern_as_string = str(shift_pattern)
                shift_pattern_count[shift_pattern_as_string] += 1
            if leet_pattern_count != None:
                (leet_pattern, base_word) = ModelTrainingUtils.get_base_word_leet_pattern(base_word)
                leet_pattern_as_string = str(leet_pattern)
                leet_pattern_count[leet_pattern_as_string] += 1
            if base_word_count != None:
                base_word_count[base_word.lower()] += 1
            total_passwords += 1
    return total_passwords, ilegal_passwords

def count_to_distribution(count_dict: defaultdict):
    """
        Converts a dict of {key: count} to a distribution dict of {key: p} where p = count_dict[key] / sum(count_dict.values)

        Args:
            count_dict: A python dictionary of {key: count} pairs 

        Returns:
            A dictionary of {key: p} pairs where Sum(dict.keys()) == 1
    """
    if not count_dict == None:
        distribution_dict = {}
        total_size = sum(count_dict.values())
        for word in count_dict.keys():
            distribution_dict[word] = count_dict[word] / total_size
        return distribution_dict
    
# def get_top_n_distribution_dict(dict: dict, n: int):
#     all_dict_sorted_by_dist
    

def create_count_dictionaries(path: str, country: str, save_count_dict: bool, destination_path: str):
    """
        Creates a count dicitonary of {password: number_of_occurences} for each of the following:
            1. Prefixes
            2. Base words
            3. Suffixes
            4. Shift patterns
            5. Leet patterns
        Args:
            path: Path of the passowrds
            country: The country of the passwords
            save_count_dict: If True, saves the count dictionaries to a file
            destination_path: The path to save the count dictionaries to
    """
    prefix_count = defaultdict(int)
    base_word_count = defaultdict(int) 
    suffix_count = defaultdict(int) 
    shift_pattern_count = defaultdict(int) 
    leet_pattern_count = defaultdict(int) 
    data_path = os.path.join(path, country)
    ilegal_passwords = 0
    total_passwords = 0
    for root, _, files in os.walk(data_path):
        for file in files:
            if country or str.endswith(file, "labeled_data.json"):
                file_path = os.path.join(root, file)
                added_total_passwords, added_ilegal_passwords = enrich_counts_from_files(file_path, base_word_count, prefix_count, suffix_count, shift_pattern_count, leet_pattern_count)
                total_passwords += added_total_passwords
                ilegal_passwords += added_ilegal_passwords
    count_dics = [("prefix_count", prefix_count), ("base_word_count", base_word_count), ("suffix_count", suffix_count), ("shift_pattern_count", shift_pattern_count), ("leet_pattern_count", leet_pattern_count)]
    data_list = []
    for dic_name, dic in count_dics:
        data_list.append({
            "name": dic_name,
            "total_size": len(dic),
            'data': dic
        })
    if save_count_dict:
        with open(os.path.join(destination_path, "count_dict.json"), 'w+') as file:
            json.dump(data_list, file, indent=4)
    with open(os.path.join(destination_path, "model_size.txt"), 'w+') as file:
        file.write(f"Total passwords: {total_passwords}\n")
        file.write(f"Ilegal passwords: {ilegal_passwords}")
    return data_list

def create_probability_disribution(count_dics: list[dict[str, any]], destination_path, ratio: int = 500):
    """
        Calculates the probability distribution of all the passwords under the provided path \n
        Args:
            path: Path of the passowrds
            country: The country of the passwords
            ratio: The ratio of the top n words to the total number of words. If ratio == 1, all the words will be saved.
    """
    meta_data = []
    for index, dic_data in enumerate(count_dics):
        count_dic = dic_data["data"]
        dict_size = dic_data["total_size"]
        distribution_dict = count_to_distribution(count_dic)
        save_distribution(distribution_dict, f"a{index + 1}", destination_path)
        n = dict_size // ratio if dict_size > (ratio * 2) else 2
        if ratio > 1:
            distribution_dict = get_top_n_values(n, distribution_dict)
            save_distribution(distribution_dict, f"{ratio}_a{index + 1}", destination_path)
            meta_data.append({
                "name": dic_data["name"],
                "n": n,
                "p": sum(distribution_dict.values())
            })
    with open(os.path.join(destination_path, "meta_data.json"), 'w+') as file:
        json.dump(meta_data, file, indent=4)

def get_top_n_values(n: int, distribution_dict: dict):
    """
        Returns a dictionary of the top n values in the provided dictionary
    """
    top_n_values = heapq.nlargest(n, distribution_dict.items(), key=lambda item: item[1])
    top_n_values_dict = {key: value for key, value in top_n_values}
    return top_n_values_dict

def execute(country: str):
    """
        Creates the sub model for each country for each ratio. If needed, creates the count dictionaries.
    """
    base_path = "C:\Country_Data"
    destination_base_path = "C:\School_data\distributions"
    destination_path = os.path.join(destination_base_path, country)
    Path(destination_path).mkdir(parents=True, exist_ok=True)
    create_count_dictionaries(base_path, country, True, destination_path)

def count_dict_to_distribution_dict(country: str, destination_base_path: str, base_path, load_from_file: bool):
    """
        Creates the sub model for each country for each ratio. If needed, creates the count dictionaries.
    """
    destination_path = os.path.join(destination_base_path, country)
    Path(destination_path).mkdir(parents=True, exist_ok=True)
    count_dics = json.load(open(os.path.join(destination_path, "count_dict.json"), 'r')) if load_from_file else create_count_dictionaries(base_path, country, True, destination_path)
    for ratio in [1000, 500, 200, 100]:
        create_probability_disribution(count_dics, destination_path, ratio)

def runAsync():
    """
        An async version of the main function
    """
    num_processes = multiprocessing.cpu_count()
    # print start time
    print("start: " + str(datetime.datetime.now()))
    countries = ["China", "Poland", "United Kingdom (common practice)", "Italy", "India", "France", "Germany", "Japan"]
    destination_base_path = "C:\School_data\distributions"
    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.map(execute, countries)
    for country in countries:
        count_dict_to_distribution_dict(country, destination_base_path, "",True)
    print("end: " + str(datetime.datetime.now()))

def runSync(load_from_file: bool = False):
    """"
        A sync version of the main function
    """
    # print start time
    print("start: " + str(datetime.datetime.now()))
    base_path = "C:\Country_Data"
    destination_base_path = "C:\School_data\distributions"
    countries = ["China", "Poland", "United Kingdom (common practice)", "Italy", "India", "France", "Germany", "Japan"]
    for country in countries:
        count_dict_to_distribution_dict(country, destination_base_path, base_path, load_from_file)
    print("end: " + str(datetime.datetime.now()))


def main():
    """
        Creates the sub model for each country for each ratio. If needed, creates the count dictionaries.
        Usage: python CreateProbabilityDistribution.py [async/sync] [load_from_file]
        Args:
            async/sync: If async, the program will run in parallel. If sync, the program will run in serial.
            load_from_file: If True, the program will load the count dictionaries from a file. If False, the program will create the count dictionaries.
    """
    isAsync = sys.argv[1] == "async"
    load_from_file = (sys.argv[2]).lower() == "true"
    if isAsync:
        runAsync()
    else:
        runSync(load_from_file)

if __name__ == "__main__":
    main()
