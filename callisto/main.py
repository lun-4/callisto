import json
import pprint
import sys
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any


class ValueType(Enum):
    map = auto()
    array = auto()
    array_union = auto()  # represent multiple types of elements inside an array
    empty_array = auto()  # represent not enough data
    string = auto()
    integer = auto()
    snowflake = auto()
    redacted = auto()


@dataclass
class Schema:
    type: ValueType
    value: Any = None

    def merge(self, other):
        if self.type == ValueType.snowflake:
            return False
        return False

    def as_json(self):
        if self.type == ValueType.map:
            return {
                "type": self.type.name,
                "schema": {k: v.as_json() for k, v in self.value.items()},
            }
        return {
            "type": self.type.name,
            "schema": self.value.as_json()
            if isinstance(self.value, Schema)
            else self.value,
        }


def deduce_structure_old(json_data):
    if isinstance(json_data, dict):
        schema = {}
        for key in json_data:
            # dont leak user info lmao
            if key in (
                "nick",
                "name",
                "username",
                "discriminator",
                "avatar",
                "email",
                "bio",
                "session_id",
                "icon",
                "country_code",
                "analytics_token",
            ):
                schema[key] = "<REDACTED>"
            else:
                schema[key] = deduce_structure(json_data[key])
        return schema
    elif isinstance(json_data, list):
        # assume first element contains the rest of the list
        # TODO find a way to attach semantic information to schema, such as
        # outliers of a schema in a certain list

        # TODO if one of the fields in the value is null, we should
        # attempt to deduce the field across other objects, and if we
        # find a value, wrap the result in Optional<T>
        #
        # this requires semantic info
        if json_data:
            return [deduce_structure(json_data[0]), "..."]
        else:
            return ["???"]
    elif isinstance(json_data, str):
        try:
            value = int(json_data)
            if value > 1420070400000:
                return "<SNOWFLAKE>"
            return value
        except ValueError:
            return json_data
    else:
        return json_data


UNWANTED_KEYS = (
    "nick",
    "name",
    "username",
    "discriminator",
    "avatar",
    "email",
    "bio",
    "session_id",
    "icon",
    "country_code",
    "analytics_token",
)


def deduce_structure(json_data) -> Schema:
    if isinstance(json_data, dict):
        schema = {}
        for key, value in json_data.items():
            if key in UNWANTED_KEYS:
                schema[key] = Schema(ValueType.redacted)
            else:
                schema[key] = deduce_structure(value)

        return Schema(ValueType.map, schema)
    elif isinstance(json_data, list):
        schema = []
        for element in json_data:
            print(element)
            element_schema = deduce_structure(element)
            merged = False
            for single_schema in schema:
                if single_schema.merge(schema):
                    merged = True
                    break

            # if none of the given schemas merged correctly, we have a new
            # type inside this array
            if not merged:
                print(element_schema)
                schema.append(element_schema)
        else:
            return Schema(ValueType.empty_array)

        print(schema)
        assert schema
        if len(schema) == 1:  # single type in list
            return Schema(ValueType.array, schema[0])
        else:
            return Schema(ValueType.array_union, schema)

    elif isinstance(json_data, str):
        try:
            value = int(json_data)
            if value > 1420070400000:  # the minimum snowflake for discord
                return Schema(ValueType.snowflake)
        except ValueError:
            pass
        return Schema(ValueType.string, json_data)
    elif isinstance(json_data, int):
        return Schema(ValueType.integer, json_data)
    else:
        raise AssertionError(f"todo support {type(json_data)}")


def cli():
    json_path = sys.argv[1]
    with open(json_path, "r") as fd:
        json_data = json.load(fd)

    # recursively walk down the json's keys and values to extract structure

    schema = deduce_structure(json_data)
    schema_json = schema.as_json()
    pprint.pprint(schema, stream=sys.stderr)
    pprint.pprint(schema_json, stream=sys.stderr)
    print(json.dumps(schema_json))


def test_simple_inference():
    assert deduce_structure(2) == 2
    assert deduce_structure("abc") == "abc"


def test_object_inference():
    assert deduce_structure({"a": 2}) == {"a": 2}
