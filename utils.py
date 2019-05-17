def safe_division(n, d):
    return n / d if d else 0

def safe_add(k, dict):
    if k in dict.keys():
        dict[k] += 1
    else:
        dict[k] = 1
    return dict

def safe_add_list(k, v, dict):
    if k in dict.keys():
        dict[k] += [v]
    else:
        dict[k] = [v]
    return dict
