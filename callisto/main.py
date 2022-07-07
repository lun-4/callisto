import json
import pprint
import sys
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any


class ValueType(Enum):
    map = auto()
    array = auto()
    union = auto()  # represent multiple types of elements inside an array
    string = auto()
    integer = auto()
    integer_range = auto()

    # have no schema inside
    empty_array = auto()  # represent not enough data
    snowflake = auto()
    boolean = auto()
    redacted = auto()
    null = auto()


@dataclass
class IntegerRange:
    min: int
    max: int

    def as_json(self):
        return {"min": self.min, "max": self.max}


NO_SCHEMA = (
    ValueType.snowflake,
    ValueType.redacted,
    ValueType.null,
    ValueType.empty_array,
    ValueType.boolean,
)


@dataclass
class Schema:
    type: ValueType
    value: Any = None

    def merge(self, other, *, mutate=False):
        assert isinstance(other, Schema)

        # if theyre of same type but no schema, they already merged lol
        if self.type in NO_SCHEMA and self.type == other.type:
            return True

        # strings become string_range
        if self.type == ValueType.string and other.type == ValueType.string:
            if mutate:
                self.value.extend(other.value)
            return True

        # integer + integer = integer_range
        # integer_range + integer = integer_range
        if (
            self.type in (ValueType.integer, ValueType.integer_range)
            and other.type == ValueType.integer
        ):

            # if we had integer, create a new integer_range
            # if we had integer_range, update the range with the new integer
            if mutate and self.type == ValueType.integer:
                self.type = ValueType.integer_range
                self.value = IntegerRange(
                    min=min(self.value, other.value), max=max(self.value, other.value)
                )
            if mutate and self.type == ValueType.integer_range:
                self.value.min = min(self.value.min, other.value)
                self.value.max = max(self.value.max, other.value)
            return True

        # if a map gets a map, we need to recurse merge() on each k,v pair
        if self.type == ValueType.map and other.type == ValueType.map:
            merged = True
            for key in other.value:
                if key in self.value:
                    merged = merged and self.value[key].merge(
                        other.value[key], mutate=mutate
                    )
                else:
                    if mutate:
                        self.value[key] = other.value[key]

            return merged

        # if an array gets an array, attempt to merge with each element
        # if any of them works, that's where mutate happens
        if self.type == ValueType.array and other.type == ValueType.array:
            assert other.value.type != ValueType.union  # TODO support union
            if self.value.merge(other.value, mutate=False):
                self.value.merge(other.value, mutate=True)
                return True
            else:
                return False

        return False

    def __repr__(self):
        return f"<{self.type.name} {self.value!r}>"

    def as_json(self):
        if self.type == ValueType.map:
            return {
                "type": self.type.name,
                "schema": {k: v.as_json() for k, v in self.value.items()},
            }
        elif self.type == ValueType.union:
            return {
                "type": self.type.name,
                "schema": [v.as_json() for v in self.value],
            }
        else:
            if isinstance(self.value, (Schema, IntegerRange)):
                return {"type": self.type.name, "schema": self.value.as_json()}
            if not self.value:
                return {"type": self.type.name}
            return {"type": self.type.name, "schema": self.value}


UNWANTED_KEYS = (
    "nick",
    "name",
    "username",
    "discriminator",
    "avatar",
    "email",
    "bio",
    "created_at",
    "session_id",
    "icon",
    "country_code",
    "analytics_token",
    "state",
)


def deduce_structure(json_data) -> Schema:
    if json_data is None:
        return Schema(ValueType.null)

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
        if not json_data:
            return Schema(ValueType.empty_array)

        for element in json_data:
            element_schema = deduce_structure(element)
            merged = False
            for single_schema in schema:
                if single_schema.merge(element_schema, mutate=False):
                    merged = True
                    single_schema.merge(element_schema, mutate=True)
                    break

            # if none of the given schemas merged correctly, we have a new
            # type inside this array
            if not merged:
                schema.append(element_schema)

        assert schema
        if len(schema) == 1:  # single type in list
            return Schema(ValueType.array, schema[0])
        else:
            return Schema(ValueType.array, Schema(ValueType.union, schema))

    elif isinstance(json_data, str):
        try:
            value = int(json_data)
            if value > 1420070400000:  # the minimum snowflake for discord
                return Schema(ValueType.snowflake)
        except ValueError:
            pass
        return Schema(ValueType.string, [json_data])
    elif isinstance(json_data, bool):
        return Schema(ValueType.boolean)
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
    assert deduce_structure(2).type == ValueType.integer


def test_list_inference():
    schema = deduce_structure({"a": [1, 2, 3, 4, 5]})
    assert schema.type == ValueType.map
    assert schema.value["a"].type == ValueType.array
    assert schema.value["a"].value.type == ValueType.integer_range


def test_applications_list():
    schema = deduce_structure(
        [
            {
                "name": "test1",
                "id": "99999999999999999",
                "icon": "sfskgjlrg",
                "command_count": 1,
                "bot": {
                    "username": "test1",
                    "public_flags": 0,
                    "id": "99999999999999999999",
                    "discriminator": "6666",
                    "bot": True,
                    "avatar": "otiuolrgkjsdflgkj",
                },
            },
            {
                "name": "test2",
                "id": "3591868261857193847",
                "icon": "alkdfjsldkgjlkgj",
                "command_count": 1,
                "bot": {
                    "username": "test2",
                    "public_flags": 65536,
                    "id": "38571938471938561",
                    "discriminator": "1321",
                    "bot": True,
                    "avatar": "ghjklrkgj",
                },
            },
        ]
    )
    assert schema.type == ValueType.array
    assert schema.value.type == ValueType.map


def test_guild_folder():
    schema = deduce_structure(
        [
            {
                "guild_ids": [
                    "99999999999999999999",
                    "99999999999999999999",
                    "99999999999999999999",
                ],
            },
            {
                "guild_ids": [
                    "99999999999999999999",
                    "99999999999999999999",
                    "99999999999999999999",
                ],
            },
        ]
    )
    assert schema.type == ValueType.array
    assert schema.value.type == ValueType.map


if __name__ == "__main__":
    cli()
