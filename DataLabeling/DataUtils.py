import tldextract
from world.database import Database # Source: https://gitlab.com/warsaw/world
import json
from Utils import save_json_array_to_file # Source: https://github.com/john-kurkowski/tldextract

def is_ascii(s):
    return all(ord(char) < 128 for char in s)

def create_origin_label(path, domains_count):
    """
        Gets a path to a file containg rows of 'email:password' pairs and returns an array of json of the following format:
        {{\n
            email: "email",\n
            password: "password"\n
            country: "country"\n
        }}
    """
    json_array = []
    file = open(path, 'rb')
    emailPasswordPairs = file.readlines()
    db = Database()
    for emailPasswordPair in emailPasswordPairs:
        try:
            string_content = emailPasswordPair.decode('unicode_escape')
            [email, password] = parse_email_password(string_content)
        except:
            continue
        if not is_ascii(password):
            continue
        try:
            tld = parse_domain(email)
            country = db.lookup_code(tld)
        except:
            country = "null"
        curr_json = {
            "email": email,
            "password": password,
            "country": country
        }
        json_array.append(curr_json)
        domains_count[country] += 1
    return json_array
    

def parse_email_password(str):
    """
        Gets an "email:password" or "email;password" string and returns an array of [email, password].
        If can't split string on ':'/';', return null
    """
    try:
        [email, password] = str.split(":", 1)
    except:
        try:
            [email, password] = str.split(";", 1)
        except:
            return
    return [email, password]

def parse_domain(email):
    """
        Gets an email address and returns its tld (top-level domain), e.g name@email.co.il => il
    """
    extract_result = tldextract.extract(email)
    suffix = extract_result.suffix 
    tld = suffix.split(".")[-1]
    return tld

# create_origin_label(path)

def aggregate_meta_data_from_labeled_data(path, domains_count):
    """
        Enriches a domains_count of {domain: #_of_passwords} with the data saves under the provided path
    """
    with open(path, "r") as read_file:
        array = json.load(read_file)
        for user in array:
            json_user = json.loads(user)
            country = json_user["country"]
            domains_count[country] += 1

def aggregate_meta_data_from_meta_data(path, domains_count):
    """
        Agrregates data from the meta_data in the provided path to the domains_count provided dictionary
    """
    with open(path, "r") as read_file:
        curr_json = json.load(read_file)
        for country in curr_json:
            domains_count[country] += curr_json[country]

def enrich_country_dict(destination_path: str, path: str, country: str, country_data: list, MAX_FILE_ENTRIES: int, file_index: int):
    """
        Adds the data of users from "country" to the provided country_data list. If len(country_data) = MAX_FILE_ENTRIES, the list is saved to the provided destination_path and a new list is generated.
        
        destination_path: The path of the file to be created if country_data is full
        path: The path of the data file being scaned
        country: The country to get users from
        country_data: An array of the form [{email:userName, password:password, country:country},...] of users from "country"
        MAX_FILE_ENTRIES: The max number of entries in a file
        file_index: The index of the current country's data file being created
    """
    with open(path, "r") as read_file:
        try:
            curr_json = json.load(read_file)
            for user in curr_json:
                if user['country'] == country and  not is_short_and_not_date(user['password']):
                    if len(country_data) >= MAX_FILE_ENTRIES:
                        save_json_array_to_file(country_data, destination_path + "" f"/{country}" + f"/{country}_{file_index}.json")
                        file_index += 1
                        country_data = []
                    country_data.append(user)
        except Exception as e:
            print(e, path)

    return (country_data, file_index)

def is_short_and_not_date(password):
    """
        Allow all-digits password to be 6 chars or longer (to represent dates).Else, 8 chars or longer.
    """
    if any(c.isalpha() for c in password):
        return len(password) < 8
    return len(password) < 6