def calculate_specific_country_tweaked_prob_factor(country, path, dimension):
    """
        To be calcullated once offline.
        Sums the probabilities of the 10,000 most popular password of country in the general distribution = p_0.
        Then, sums the probability of these passwords in the country's specific distribution = p
        Returns the tweaking factor by this calculation: 1 - (p - p_0)
    """
    path = os.path.join(path, country, dimension + ".txt")
    p_0, p = 0, 0
    with open(path, "r") as fp:
        # TODO - check splitting part of password, prob lines
        line = fp.readline().strip().split()
        if len(line) == 1:
            word = ""
            probability = line[0]
        else:
            password = line[0]
            for i in line[1:-1]:
                password = password+" "+i
            word = password
            probability = line[-1]
        # TODO - add some assertions that the word exists in the general dist and that for each word p > p_0
        p += probability
        word_general_p = BS.main(path, word) 
        p_0 += word_general_p if word_general_p else 0
    return 1 - (p - p_0)

def calculate_complete_tweaked_prob_factor(country, path):
    dimensions = ["a1", "a2", "a3", "a4", "a5"]
    tweking_factor = 1
    for dimension in dimensions:
        tweking_factor *= calculate_specific_country_tweaked_prob_factor(country, path, dimension)
    return tweking_factor