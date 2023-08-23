import heapq
import os
from collections import defaultdict
import PasswordDetailsUtils
import json
from pathlib import Path

def save_distribution(data: dict, file_name: str, path: str):
    """
        Saves the dictionary in the provided path with the provided name

        Args:
            data: A dictionary of {key: p} pairs where Sum(dict.keys()) == 1
            file_name: The desiered name to the file created
            path: The path to the file created
    """
    Path(path).mkdir(parents=True, exist_ok=True)
    file_name = path + "\\" + file_name + ".txt"
    sorted_data = dict(sorted(data.items()))

    with open(file_name, "w") as file :
        for key,value in sorted_data.items():
            file.write(f"{repr(key)[1:-1]} {value}\n")

def enrich_counts_from_files(file_path: str, base_word_count: defaultdict, prefix_count: defaultdict, suffix_count: defaultdict, shift_pattern_count: defaultdict, leet_pattern_count: defaultdict, ilegal_passwords: int):
    """
        Enriches the provided dictionaries according to the passowrds in the provided path
    """
    with open(file_path, "r") as read_file:
        array = json.load(read_file)
        for user in array:
            password = user['password']
            if not PasswordDetailsUtils.is_leagal_password(password):
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
    return ilegal_passwords

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
    

def create_count_dictionaries(path: str, country: str, save_count_dict: bool):
    prefix_count = defaultdict(int)
    base_word_count = defaultdict(int) 
    suffix_count = defaultdict(int) 
    shift_pattern_count = defaultdict(int) 
    leet_pattern_count = defaultdict(int) 
    dest_path = os.path.join(path, country)
    ilegal_passwords = 0
    for root, _, files in os.walk(dest_path):
        for file in files:
            if country or str.endswith(file, "labeled_data.json"):
                file_path = os.path.join(root, file)
                ilegal_passwords = enrich_counts_from_files(file_path, base_word_count, prefix_count, suffix_count, shift_pattern_count, leet_pattern_count, ilegal_passwords)
    count_dics = [("prefix_count", prefix_count), ("base_word_count", base_word_count), ("suffix_count", suffix_count), ("shift_pattern_count", shift_pattern_count), ("leet_pattern_count", leet_pattern_count)]
    data_list = []
    for dic_name, dic in count_dics:
        data_list.append({
            "name": dic_name,
            "total_size": len(dic),
            'data': dic
        })
    if save_count_dict:
        with open(os.path.join(dest_path, "count_dict.json"), 'w+') as file:
            json.dump(data_list, file, indent=4)    
    print(f"Ilegal passwords: {ilegal_passwords}")
    return data_list

def create_probability_disribution(count_dics: [{str, any}], dest_path: str, ratio: int = 500):
    """
        Calculates the probability distribution of all the passwords under the provided path \n
        Args:
            path: Path of the passowrds
            country: The country of the passwords
            save_count_dict: A boolean indicating if the count dictionaries should be saved to a file
    """
    meta_data = []
    for index, dic_data in enumerate(count_dics):
        count_dic = dic_data["data"]
        n = dic_data["total_size"] // ratio if dic_data["total_size"] > (ratio * 2) else 2
        distribution_dict = count_to_distribution(count_dic)
        top_n_values = heapq.nlargest(n, distribution_dict.items(), key=lambda item: item[1])
        top_n_values_dict = {key: value for key, value in top_n_values}
        save_distribution(top_n_values_dict, f"a{index + 1}", dest_path)
        meta_data.append({
            "name": dic_data["name"],
            "n": n,
            "p": sum(top_n_values_dict.values())
        })
    with open(os.path.join(dest_path, "meta_data.json"), 'w+') as file:
        json.dump(meta_data, file, indent=4)


# countries = ["Ukraine"] #["Ukraine", "China", "Poland", "United Kingdom (common practice)", "France", "Germany", "Japan"]
# base_path = "C:\\Users\\nirfi\\Desktop\\data_by_country\\new_data"
# for root, directories, files in os.walk(base_path):
#     for directory in directories:
#         create_probability_disribution(os.path.join(base_path, directory), "China", True)

def main():
    base_path = "C:\\Users\\nirfi\\Desktop\\data_by_country\\new_data"
    countries = ["China", "France", "Germany", "Japan", "Poland", "United Kingdom (common practice)", "Italy", "India"]
    for country in countries:
        with open(os.path.join(base_path, country, "count_dict.json"), 'r') as file:
            meta_data = json.load(file)
            #count_dics = create_count_dictionaries(base_path, country, True)
            create_probability_disribution(meta_data, os.path.join(base_path, country))

if __name__ == "__main__":
    main()