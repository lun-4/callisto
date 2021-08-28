import json
import pprint
import sys


def deduce_structure(json_data):
    if isinstance(json_data, dict):
        schema = {}
        for key in json_data:
            # dont leak user info lmao
            if key in ("nick", "name"):
                schema[key] = "<REDACTED>"
            else:
                schema[key] = deduce_structure(json_data[key])
        return schema
    elif isinstance(json_data, list):
        # assume first element contains the rest of the list
        # TODO find a way to attach semantic information to schema, such as
        # outliers of a schema in a certain list
        if json_data:
            return [deduce_structure(json_data[0]), "..."]
        else:
            return ["???"]  # we cant deduce structure from empty lists
    elif isinstance(json_data, str):
        try:
            int(json_data)
            return "<SNOWFLAKE>"
        except ValueError:
            return json_data
    else:
        return json_data


def cli():
    json_path = sys.argv[1]
    with open(json_path, "r") as fd:
        json_data = json.load(fd)

    # recursively walk down the json's keys and values to extract structure

    schema = deduce_structure(json_data)
    pprint.pprint(schema)
    print(json.dumps(schema))
