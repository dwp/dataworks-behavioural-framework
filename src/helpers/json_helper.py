def remove_key_from_dict(data, key):
    """Search key in dictionary and remove if found
    Keyword arguments:
    data -- a dictionary
    key -- key to delete
    """
    data_copy = data.copy()
    for k, v in data_copy.items():
        if k == key:
            data.pop(k)
        if isinstance(v, dict):
            remove_key_from_dict(data[k], key)


def replace_value_from_dict_using_key(data, key, new_value):
    """Search key in dictionary and remove if found
    Keyword arguments:
    data -- a dictionary
    key -- key to delete
    new_value -- replacement value
    """
    for k, v in data.items():
        if k == key:
            data[k] = new_value
        if isinstance(v, dict):
            replace_value_from_dict_using_key(data[k], key, new_value)
