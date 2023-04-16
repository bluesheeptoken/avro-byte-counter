# Getting started

- git clone project
- python3 -m pip install .
- run following script
```python3
from avro.io import *
import io

from avro_byte_counter.avro_byte_counter import AvroByteCounter

schema = avro.schema.parse("""{
    "type": "record",
    "name": "Person",
    "fields": [
        {"name": "userName",       "type": "string"},
        {"name": "favoriteNumber", "type": ["null", "long"], "default": null},
        {"name": "interests",      "type": {"type": "array", "items": "string"}}
    ]
}
""")

# Write bytes
writer = DatumWriter(schema)
bytes_writer = io.BytesIO()
writer.write(
    {"userName": "Martin", "favoriteNumber": 1337, "interests": ["hacking", "daydreaming"]},
    BinaryEncoder(bytes_writer)
)

# Count bytes per field
raw_bytes = bytes_writer.getvalue()
avro_byte_counter = AvroByteCounter(raw_bytes, schema)
byte_count_per_field = avro_byte_counter.count_byte_per_field()
print(byte_count_per_field)
```

For flamegraph compatible format, run:

```python
from avro_byte_counter.utils import count_to_flamegraph_format
print(*count_to_flamegraph_format(byte_count_per_field), sep='\n')
```