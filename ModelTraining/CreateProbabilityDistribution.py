import datetime
import heapq
import os
from collections import defaultdict
import PasswordDetailsUtils
import json
from pathlib import Path
import multiprocessing

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
            if (not PasswordDetailsUtils.is_leagal_password(password)) or (PasswordDetailsUtils.is_short_and_not_date(password)):
                ilegal_passwords += 1
                continue
            [prefix, base_word, suffix] = PasswordDetailsUtils.parse_password_to_3d(password)
            if suffix_count != None:
                suffix_count[suffix] += 1
            if prefix_count != None:
                prefix_count[prefix] += 1
            if shift_pattern_count != None:
                shift_pattern = PasswordDetailsUtils.get_base_word_shift_pattern(base_word)
                if len(shift_pattern) == len(base_word):
                    shift_pattern_as_string = "all-cap"
                shift_pattern_as_string = str(shift_pattern)
                shift_pattern_count[shift_pattern_as_string] += 1
            if leet_pattern_count != None:
                (leet_pattern, base_word) = PasswordDetailsUtils.get_base_word_leet_pattern(base_word)
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
    top_n_values = heapq.nlargest(n, distribution_dict.items(), key=lambda item: item[1])
    top_n_values_dict = {key: value for key, value in top_n_values}
    return top_n_values_dict


# countries = ["Ukraine"] #["Ukraine", "China", "Poland", "United Kingdom (common practice)", "France", "Germany", "Japan"]
# base_path = "C:\\Users\\nirfi\\Desktop\\data_by_country\\new_data"
# for root, directories, files in os.walk(base_path):
#     for directory in directories:
#         create_probability_disribution(os.path.join(base_path, directory), "China", True)

def execute(country: str):
    base_path = "C:\Country_Data"
    destination_base_path = "C:\School_data\distributions"
    destination_path = os.path.join(destination_base_path, country)
    Path(destination_path).mkdir(parents=True, exist_ok=True)
    count_dics = create_count_dictionaries(base_path, country, True, destination_path)
    create_probability_disribution(count_dics, destination_path, 500)

def main():
    num_processes = multiprocessing.cpu_count()
    # print start time
    print("start: " + str(datetime.datetime.now()))

    countries = ["China", "Poland", "United Kingdom (common practice)", "Italy", "India", "France", "Germany", "Japan"]
    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.map(execute, countries)

    # count_dics = create_count_dictionaries(base_path, "", True)
    # create_probability_disribution(count_dics, base_path, 1)
    # print("end: " + str(datetime.datetime.now()))




# def main():
#     # print start time
#     print("start: " + str(datetime.datetime.now()))
#     base_path = "C:\Country_Data"
#     destination_base_path = "C:\School_data\distributions"
#     countries = ["China", "Poland", "United Kingdom (common practice)", "Italy", "India", "France", "Germany", "Japan"]
#     for country in countries:
#         destination_path = os.path.join(destination_base_path, country)
#         Path(destination_path).mkdir(parents=True, exist_ok=True)
#         count_dics = create_count_dictionaries(base_path, country, True, destination_path)
#         create_probability_disribution(count_dics, destination_path, 500)
#     # count_dics = create_count_dictionaries(base_path, "", True)
#     # create_probability_disribution(count_dics, base_path, 1)
#     # print("end: " + str(datetime.datetime.now()))

if __name__ == "__main__":
    main()