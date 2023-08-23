def sort_meta_data(data: dict):
    sorted_data = dict(sorted(data.items(), key=lambda item: item[1], reverse=True))
    return sorted_data