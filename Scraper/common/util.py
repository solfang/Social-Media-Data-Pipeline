import json

def save_json(filename, data, indent=0):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=indent)


def read_json(filename):
    with open(filename) as json_file:
        return json.load(json_file)