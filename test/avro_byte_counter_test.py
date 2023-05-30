import io
import json
import unittest

import avro
import pytest
from avro.io import DatumWriter, BinaryEncoder

from src.avro_byte_counter.avro_byte_counter import AvroByteCounter


def prepare_bytes(datum: dict, schema: avro.schema.Schema) -> bytes:
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(datum, encoder)
    raw_bytes = bytes_writer.getvalue()
    return raw_bytes


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
        avro_byte_counter = AvroByteCounter(schema)
        raw_bytes = prepare_bytes(datum, schema)
        self.assertEqual(
            {
                "userName": 7,
                "favoriteNumber": {"union_branch": 1, "value": 2},
                "interests": {"array_overhead": 2, "values": 20},
            },
            avro_byte_counter.count_byte_per_field(raw_bytes),
        )
        self.assertEqual(32, len(raw_bytes))


class AvroByteCounterUnionTest(unittest.TestCase):
    def setUp(self):
        self.schema = avro.schema.parse(
            """
            {
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
            }
            """
        )

    def test_null_branch(self):
        raw_bytes = prepare_bytes({"union": None}, self.schema)
        avro_byte_counter = AvroByteCounter(self.schema)
        self.assertEqual(
            {"union": {"union_branch": 1, "value": 0}},
            avro_byte_counter.count_byte_per_field(raw_bytes),
        )
        self.assertEqual(1, len(raw_bytes))

    def test_record_branch(self):
        raw_bytes = prepare_bytes({"union": {"value": 5}}, self.schema)
        avro_byte_counter = AvroByteCounter(self.schema)
        self.assertEqual(
            {"union": {"union_branch": 1, "value": {"value": 1}}},
            avro_byte_counter.count_byte_per_field(raw_bytes),
        )
        self.assertEqual(2, len(raw_bytes))


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
        raw_bytes = prepare_bytes({"enumValue": "val_b"}, self.schema)
        avro_byte_counter = AvroByteCounter(self.schema)
        self.assertEqual(
            {"enumValue": 1},
            avro_byte_counter.count_byte_per_field(raw_bytes),
        )
        self.assertEqual(1, len(raw_bytes))


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
        raw_bytes = prepare_bytes({"arrayValue": [0, 1, 2]}, schema)
        avro_byte_counter = AvroByteCounter(schema)
        self.assertEqual(
            {"arrayValue": {"array_overhead": 2, "values": 3}},
            avro_byte_counter.count_byte_per_field(raw_bytes),
        )
        self.assertEqual(5, len(raw_bytes))

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
        raw_bytes = prepare_bytes(
            {"arrayValue": [{"nestedRecordValue": 1}, {"nestedRecordValue": 2}]}, schema
        )
        avro_byte_counter = AvroByteCounter(schema)
        self.assertEqual(
            {
                "arrayValue": {
                    "array_overhead": 2,
                    "values": {"nestedRecordValue": 2},
                }
            },
            avro_byte_counter.count_byte_per_field(raw_bytes),
        )
        self.assertEqual(4, len(raw_bytes))


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
        raw_bytes = prepare_bytes(
            {"mapValues": {"key1": "value1", "key2": "value2"}}, schema
        )
        avro_byte_counter = AvroByteCounter(schema)
        self.assertEqual(
            {"mapValues": {"keys": 10, "items": 14, "overhead": 2}},
            avro_byte_counter.count_byte_per_field(raw_bytes),
        )
        self.assertEqual(len(raw_bytes), 26)

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
        raw_bytes = prepare_bytes(
            {"mapValues": {"key1": {"value": 1}, "key2": {"value": 2}}}, schema
        )
        avro_byte_counter = AvroByteCounter(schema)
        self.assertEqual(
            {
                "mapValues": {
                    "keys": 10,
                    "items": {"value": 2},
                    "overhead": 2,
                }
            },
            avro_byte_counter.count_byte_per_field(raw_bytes),
        )
        self.assertEqual(14, len(raw_bytes))


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
        raw_bytes = prepare_bytes(
            {"inner_record": {"inner_value": 200, "second_inner_value": 1}}, schema
        )
        avro_byte_counter = AvroByteCounter(schema)
        self.assertEqual(
            {
                "inner_record": {
                    "inner_value": 2,
                    "second_inner_value": 1,
                },
            },
            avro_byte_counter.count_byte_per_field(raw_bytes),
        )
        self.assertEqual(3, len(raw_bytes))


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
        raw_bytes = prepare_bytes(
            {"hash": b"\x1f\xf2\x9d\xe3L\x98\xe8\xbf\xf6-\xc6\x97Q\x1e\xf6L"}, schema
        )
        avro_byte_counter = AvroByteCounter(schema)
        self.assertEqual(
            {"hash": 16}, avro_byte_counter.count_byte_per_field(raw_bytes)
        )
        self.assertEqual(16, len(raw_bytes))


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
    raw_bytes = prepare_bytes({"H": value}, schema)
    avro_byte_counter = AvroByteCounter(schema)
    assert {"H": expected_size} == avro_byte_counter.count_byte_per_field(raw_bytes)
    assert expected_size == len(raw_bytes)


if __name__ == "__main__":
    unittest.main()
