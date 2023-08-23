import codecs
import re

def get_first_letter_index(password: str):
    """
        Gets the index of the first letter of the password
    """
    start_index = 0
    for index, char in enumerate(password):
        if char .isalpha():
            start_index = index
            break
    return start_index

def get_base_word_start_index(password: str):
    """
        Gets the index of the first character of the base word of the provided passowrd
    """
    return get_first_letter_index(password=password)

def get_suffix_start_index(password: str):
    """
        Gets the index of the first character of the suffix of the provided passowrd
    """
    index = get_first_letter_index(password=password[::-1])
    return - index if index > 0 else len(password) + 1

def parse_password_to_3d(password: str):
    """
        Parses a password to prefix, base word, suffix as described in https://arxiv.org/abs/1912.02551
    """
    base_word_start_index = get_base_word_start_index(password=password)
    base_word_end_index = get_suffix_start_index(password=password)
    prefix = password[0 : base_word_start_index]
    base_word = password[base_word_start_index : base_word_end_index]
    suffix = password[base_word_end_index : ]
    return (prefix, base_word, suffix)

def get_shift_pattern(word: str, start_from_end: bool):
    """
        Gets the shift-pattern of the provided word
    """
    pattern = []
    no_char = True
    word = word[::-1] if start_from_end else word
    for index, letter in enumerate(word):
        if str.isalpha(letter):
            no_char = False
            if not str.islower(letter):
                pattern.append(-1 - index if start_from_end else index)
    return [] if no_char else pattern

def get_base_word_shift_pattern(base_word: str):
    """
        Gets the shift pattern (Capitalization of characters) of the base word as represented in https://arxiv.org/abs/1912.02551
    """
    mid_index = len(base_word) // 2
    left_hand_pattern = get_shift_pattern(base_word[ : mid_index], False)
    right_hand_pattern = get_shift_pattern(base_word[mid_index : ], True)[::-1]
    shift_pattern = left_hand_pattern + right_hand_pattern
    return shift_pattern

def is_leagal_password(password):
    """
        Source: isascci() here - https://github.com/lirondavid/PESrank/blob/master/PESrank/PESrank.py#L47
    """
    return all(ord(c) < 128 for c in password)

def get_base_word_leet_pattern(password):
    leet_pattern = []
    unleet_password = ""
    leet_transformations_dict = {"0": (1, "o"), "@": (2, "a"), "4": (3, "a"), "$": (4, "s"), "5": (5, "s"), "3": (6, "e"), 6: (7, "g"), 9: (8, "g"), "+": (9, "t"), "7": (10, "t"), "2": (11, "z"), "1": (12, "i"), "!": (13, "i"), "%": (14, "x")}
    unleet_letters = set()
    for (_, letter) in enumerate(password):
        if letter in leet_transformations_dict.keys():
            (index, unleet_letter) = leet_transformations_dict[letter]
            unleet_password += unleet_letter
            if unleet_letter in unleet_letters:
                continue
            unleet_letters.add(unleet_letter)
            leet_pattern.append(index)
        else:
            unleet_password += letter
    return tuple(leet_pattern), unleet_password

def escape_password(password: str):
    escape_password = codecs.encode(password, 'unicode_escape')
    return escape_password

# a = "asdasd"
# b = codecs.encode(a, 'unicode_escape')
# d = codecs.decode(a, 'unicode_escape')
# c = 1 + 1


