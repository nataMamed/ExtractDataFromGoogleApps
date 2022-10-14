import json

def read_json(path):
    with open(path, 'r') as infile:
        data =json.load(infile)
    return data
