import io
from typing import Dict, Tuple

from avro.io import BinaryDecoder

from avro_byte_counter.type import CountPerField
from avro_byte_counter.utils import merge_counts
from avro.schema import (
    Schema,
    UnionSchema,
    FixedSchema,
    EnumSchema,
    ArraySchema,
    MapSchema,
    RecordSchema,
)


class AvroByteCounter:
    def __init__(self, schema: Schema):
        self.__schema: Schema = schema

    def count_byte_per_field(self, raw_bytes: bytes) -> CountPerField:
        self.__decoder = BinaryDecoder(io.BytesIO(raw_bytes))
        return self._count_data(self.__schema)  # pyright: ignore

    def _count_data(self, schema: Schema) -> object:
        if isinstance(schema, UnionSchema):
            return self._count_union(schema)
        if schema.type == "null":
            return self._count_null()
        if schema.type == "boolean":
            return self._count_boolean()
        if schema.type == "string":
            return self._count_utf8()
        if schema.type == "int":
            return self._count_int()
        if schema.type == "long":
            return self._count_long()
        if schema.type == "float":
            return self._count_float()
        if schema.type == "double":
            return self._count_double()
        if schema.type == "bytes":
            return self._count_bytes()
        if isinstance(schema, FixedSchema):
            return self._count_fixed_schema(schema)
        if isinstance(schema, EnumSchema):
            return self._count_enum()
        if isinstance(schema, ArraySchema):
            return self._count_array(schema)
        if isinstance(schema, MapSchema):
            return self._count_map(schema)
        if isinstance(schema, RecordSchema):
            return self._count_record(schema)
        # Should never happen
        raise Exception(f"Unexpected type for avro's Schema type: {schema}")

    def _count_union(self, schema: UnionSchema) -> Dict[str, object]:
        """
        A union is encoded by first writing an int value indicating
        the zero-based position within the union of the schema of its value.
        The value is then encoded per the indicated schema within the union.
        """
        # schema resolution
        (count, index_of_schema) = self._count_and_read_long()
        selected_schema = schema.schemas[index_of_schema]
        # read data
        return {"union_branch": count, "value": self._count_data(selected_schema)}

    def _count_null(self) -> int:
        """
        null is written as zero bytes
        """
        return 0

    def _count_record(self, schema: RecordSchema) -> Dict[str, object]:
        """
        A record is encoded by encoding the values of its fields
        in the order that they are declared. In other words, a record
        is encoded as just the concatenation of the encodings of its fields.
        Field values are encoded per their schema.
        """
        readers_fields_dict = schema.fields_dict
        read_bytes = {}
        for field in schema.fields:
            readers_field = readers_fields_dict.get(field.name)
            if readers_field is not None:
                read_bytes[field.name] = self._count_data(readers_field.type)
            else:
                pass
        return read_bytes

    def _count_utf8(self) -> int:
        """
        A string is encoded as a long followed by
        that many bytes of UTF-8 encoded character data.
        """
        return self._count_bytes()

    def _count_bytes(self) -> int:
        """
        Bytes are encoded as a long followed by that many bytes of data.
        """
        (count, size) = self._count_and_read_long()
        self.__decoder.read(size)
        return count + size

    def _count_and_read_long(self) -> Tuple[int, int]:
        """
        int and long values are written using variable-length, zig-zag coding.
        """
        count = 0
        b = ord(self.__decoder.read(1))
        n = b & 0x7F
        shift = 7
        count += 1
        while (b & 0x80) != 0:
            b = ord(self.__decoder.read(1))
            n |= (b & 0x7F) << shift
            shift += 7
            count += 1
        datum = (n >> 1) ^ -(n & 1)
        return count, datum

    def _count_int(self) -> int:
        return self._count_long()

    def _count_long(self) -> int:
        count, value = self._count_and_read_long()
        return count

    def _count_float(self) -> int:
        """
        A float is written as 4 bytes.
        """
        return self._count_and_skip_fix_number_of_bytes(4)

    def _count_double(self) -> int:
        """
        A double is written as 8 bytes.
        """
        return self._count_and_skip_fix_number_of_bytes(8)

    def _count_boolean(self) -> int:
        """
        a boolean is written as a single byte
        whose value is either 0 (false) or 1 (true).
        """
        return self._count_and_skip_fix_number_of_bytes(1)

    def _count_decimal_bytes(self) -> int:
        """
        Decimal bytes are decoded as signed short, int or long depending on the
        size of bytes.
        """
        return self._count_bytes()

    def _count_enum(self) -> int:
        """
        An enum is encoded by an int, representing the zero-based position
        of the symbol in the schema.
        """
        return self._count_long()

    def _count_array(self, schema: ArraySchema) -> Dict[str, object]:
        """
        Arrays are encoded as a series of blocks.

        Each block consists of a long count value,
        followed by that many array items.
        A block with count zero indicates the end of the array.
        Each item is encoded per the array's item schema.

        If a block's count is negative,
        then the count is followed immediately by a long block size,
        indicating the number of bytes in the block.
        The actual count in this case
        is the absolute value of the count written.
        """
        items_count = []
        overhead_count = 0
        (block_count_count, block_count) = self._count_and_read_long()
        overhead_count += block_count_count
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                (block_size_count, block_size) = self._count_and_read_long()
                overhead_count += block_size_count
            for i in range(block_count):
                items_count.append(self._count_data(schema.items))
            (block_count_count, block_count) = self._count_and_read_long()
            overhead_count += block_count_count
        if self._is_primitive_type(schema.items):
            items_values = sum(items_count)
        else:
            items_values = merge_counts(items_count)
        return {"array_overhead": overhead_count, "values": items_values}

    def _count_map(self, schema: MapSchema) -> Dict[str, object]:
        """
        Maps are encoded as a series of blocks.

        Each block consists of a long count value,
        followed by that many key/value pairs.
        A block with count zero indicates the end of the map.
        Each item is encoded per the map's value schema.

        If a block's count is negative,
        then the count is followed immediately by a long block size,
        indicating the number of bytes in the block.
        The actual count in this case
        is the absolute value of the count written.
        """
        items_count = []
        overhead_count = 0
        keys_count = 0

        (block_count_count, block_count) = self._count_and_read_long()
        overhead_count += block_count_count

        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                (block_size_count, block_size) = self._count_and_read_long()
                overhead_count += block_size_count
            for i in range(block_count):
                keys_count += self._count_utf8()
                items_count.append(self._count_data(schema.values))
            (block_count_count, block_count) = self._count_and_read_long()
            overhead_count += block_count_count
        if self._is_primitive_type(schema.values):
            items_values = sum(items_count)
        else:
            items_values = merge_counts(items_count)
        return {"overhead": overhead_count, "keys": keys_count, "items": items_values}

    def _count_fixed_schema(self, schema: FixedSchema) -> int:
        """
        Fixed instances are encoded using the number of bytes declared
        in the schema.
        """
        return self._count_and_skip_fix_number_of_bytes(schema.size)

    def _count_and_skip_fix_number_of_bytes(self, number_of_bytes: int):
        self.__decoder.skip(number_of_bytes)
        return number_of_bytes

    def _is_primitive_type(self, schema: Schema) -> bool:
        return schema.type in (
            "null",
            "boolean",
            "string",
            "int",
            "long",
            "float",
            "double",
            "bytes",
        )
