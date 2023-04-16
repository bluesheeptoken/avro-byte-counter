import io
import json
import unittest

import avro
import pytest
from avro.io import DatumWriter, BinaryEncoder, BinaryDecoder

from src.avro_byte_counter.avro_byte_counter import AvroByteCounter


def prepare_bytes(datum: dict, schema: avro.schema.Schema) -> AvroByteCounter:
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(datum, encoder)
    raw_bytes = bytes_writer.getvalue()
    bytes_reader = io.BytesIO(raw_bytes)
    decoder = BinaryDecoder(bytes_reader)
    return AvroByteCounter(decoder)


class AvroByteCounterRecordTest(unittest.TestCase):
    def test_nominal_case(self):
        # cf DDIA avro example from Martin's Kleppmann: https://martin.kleppmann.com/2012/12/05/schema-evolution-in-avro-protocol-buffers-thrift.html
        datum = {
            "userName": "Martin",
            "favoriteNumber": 1337,
            "interests": ["hacking", "daydreaming"],
        }
        schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "Person",
                    "fields": [
                        {"name": "userName", "type": "string"},
                        {
                            "name": "favoriteNumber",
                            "type": ["null", "long"],
                            "default": None,
                        },
                        {
                            "name": "interests",
                            "type": {"type": "array", "items": "string"},
                        },
                    ],
                }
            )
        )
        avro_byte_counter = prepare_bytes(datum, schema)
        self.assertEqual(
            {
                "userName": 7,
                "favoriteNumber": {"union_branch": 1, "value": 2},
                "interests": {"array_overhead": 1, "values": 20},
                "end_of_record": 1,
            },
            avro_byte_counter.count_data(schema),
        )


class AvroByteCounterUnionTest(unittest.TestCase):
    def setUp(self):
        self.schema = avro.schema.parse(
            """{
                    "type": "record",
                    "name": "Test",
                    "fields": [{
                        "name": "union",
                        "type": ["null",
                            {
                                "type": "record",
                                "name": "NestedRecord",
                                "fields": [{
                                    "name": "value",
                                    "type": "long"
                                }]
                            }
                        ]
                    }]
            """
        )

    def test_null_branch(self):
        datum = {"union": None}
        avro_byte_counter = prepare_bytes(datum, self.schema)
        self.assertEqual(
            {"union": {"union_branch": 1, "value": 0}, "end_of_record": 1},
            avro_byte_counter.count_data(self.schema),
        )

    def test_record_branch(self):
        datum = {"union": {"value": 5}}
        avro_byte_counter = prepare_bytes(datum, self.schema)
        self.assertEqual(
            {
                "end_of_record": 1,
                "union": {"union_branch": 1, "value": {"end_of_record": 1, "value": 1}},
            },
            avro_byte_counter.count_data(self.schema),
        )


class AvroByteCounterEnumTest(unittest.TestCase):
    def setUp(self):
        self.schema = avro.schema.parse(
            """
{
  "type": "record",
  "name": "test",
  "fields": [
    {
      "name": "enumValue",
      "type": {
        "name": "EnumType",
        "type": "enum",
        "symbols": [
          "val_a",
          "val_b"
        ]
      }
    }
  ]
}"""
        )

    def test_enum(self):
        datum = {"enumValue": "val_a"}
        avro_byte_counter = prepare_bytes(datum, self.schema)
        self.assertEqual(
            {"enumValue": 1, "end_of_record": 1},
            avro_byte_counter.count_data(self.schema),
        )


class AvroByteCounterArrayTest(unittest.TestCase):
    def test_array_primitives(self):
        schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "test",
                    "fields": [
                        {
                            "name": "arrayValue",
                            "type": {"type": "array", "items": "long"},
                        }
                    ],
                }
            )
        )
        datum = {"arrayValue": [0, 1, 2]}
        avro_byte_counter = prepare_bytes(datum, schema)
        self.assertEqual(
            {"arrayValue": {"array_overhead": 1, "values": 3}, "end_of_record": 1},
            avro_byte_counter.count_data(schema),
        )

    def test_array_records(self):
        schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "test",
                    "fields": [
                        {
                            "name": "arrayValue",
                            "type": {
                                "type": "array",
                                "items": {
                                    "type": "record",
                                    "name": "recordValue",
                                    "fields": [
                                        {"name": "nestedRecordValue", "type": "int"}
                                    ],
                                },
                            },
                        }
                    ],
                }
            )
        )
        datum = {"arrayValue": [{"nestedRecordValue": 1}, {"nestedRecordValue": 2}]}
        avro_byte_counter = prepare_bytes(datum, schema)
        self.assertEqual(
            {
                "arrayValue": {
                    "array_overhead": 1,
                    "values": {"end_of_record": 2, "nestedRecordValue": 2},
                },
                "end_of_record": 1,
            },
            avro_byte_counter.count_data(schema),
        )


class AvroByteCounterMapTest(unittest.TestCase):
    def test_map_primitives(self):
        schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "test",
                    "fields": [
                        {
                            "name": "mapValues",
                            "type": {"type": "map", "values": "string"},
                        }
                    ],
                }
            )
        )
        datum = {"mapValues": {"key1": "value1", "key2": "value2"}}
        avro_byte_counter = prepare_bytes(datum, schema)
        self.assertEqual(
            {"mapValues": {"keys": 10, "items": 14, "overhead": 2}, "end_of_record": 1},
            avro_byte_counter.count_data(schema),
        )

    def test_map_records(self):
        schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "test",
                    "fields": [
                        {
                            "name": "mapValues",
                            "type": {
                                "type": "map",
                                "values": {
                                    "type": "record",
                                    "name": "testRecord",
                                    "fields": [{"name": "value", "type": "int"}],
                                },
                            },
                        }
                    ],
                }
            )
        )
        datum = {"mapValues": {"key1": {"value": 1}, "key2": {"value": 2}}}
        avro_byte_counter = prepare_bytes(datum, schema)
        self.assertEqual(
            {
                "mapValues": {
                    "keys": 10,
                    "items": {"value": 2, "end_of_record": 2},
                    "overhead": 2,
                },
                "end_of_record": 1,
            },
            avro_byte_counter.count_data(schema),
        )


class AvroByteCounterNestedRecordsTest(unittest.TestCase):
    def test_nested_record(self):
        schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "outer_record",
                    "fields": [
                        {
                            "name": "inner_record",
                            "type": {
                                "type": "record",
                                "name": "inner_record",
                                "fields": [
                                    {"name": "inner_value", "type": "long"},
                                    {"name": "second_inner_value", "type": "long"},
                                ],
                            },
                        }
                    ],
                }
            )
        )
        datum = {"inner_record": {"inner_value": 200, "second_inner_value": 1}}
        avro_byte_counter = prepare_bytes(datum, schema)
        self.assertEqual(
            {
                "inner_record": {
                    "inner_value": 2,
                    "second_inner_value": 1,
                    "end_of_record": 1,
                },
                "end_of_record": 1,
            },
            avro_byte_counter.count_data(schema),
        )


class AvroByteCounterFixedSchemaTest(unittest.TestCase):
    def test_fixed_schema(self):
        schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "test",
                    "fields": [
                        {
                            "name": "hash",
                            "type": {"type": "fixed", "name": "md5", "size": 16},
                        }
                    ],
                }
            )
        )
        datum = {"hash": b"\x1f\xf2\x9d\xe3L\x98\xe8\xbf\xf6-\xc6\x97Q\x1e\xf6L"}
        avro_byte_counter = prepare_bytes(datum, schema)
        self.assertEqual(
            {"hash": 16, "end_of_record": 1}, avro_byte_counter.count_data(schema)
        )


@pytest.mark.parametrize(
    "field_type, value, expected_size",
    [
        ("null", None, 0),
        ("boolean", True, 1),
        ("string", "foo", 4),
        ("bytes", bytes("ba", "utf-8"), 3),
        ("int", 5, 1),
        ("long", 5, 1),
        ("long", 256, 2),
        ("float", 1.1, 4),
        ("double", 1.1, 8),
    ],
)
def test_primitive_type(field_type: str, value: object, expected_size: int):
    schema = avro.schema.parse(
        json.dumps(
            {
                "type": "record",
                "name": "Test",
                "fields": [
                    {
                        "name": "H",
                        "type": field_type,
                    }
                ],
            }
        )
    )
    avro_byte_counter = prepare_bytes({"H": value}, schema)
    assert {"H": expected_size, "end_of_record": 1} == avro_byte_counter.count_data(
        schema
    )


if __name__ == "__main__":
    unittest.main()
