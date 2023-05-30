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
avro_byte_counter = AvroByteCounter(schema)
byte_count_per_field = avro_byte_counter.count_byte_per_field(raw_bytes)
print(byte_count_per_field)
```

For flamegraph compatible format, run:

```python
from avro_byte_counter.utils import count_to_flamegraph_format
print(*count_to_flamegraph_format(byte_count_per_field), sep='\n')
```

And then you can combine the python file created with the `flamegraph.pl` script to generate a flamegraph.


```shell
curl https://raw.githubusercontent.com/brendangregg/FlameGraph/d9fcc272b6a08c3e3e5b7919040f0ab5f8952d65/flamegraph.pl # any version is fine
chmod +x flamegraph.pl
python run myscript.py | ./flamegraph.pl --countname bytes > profiles/flamegraph.svg 
```

I gladly take feedbacks you have regarding the API, the usefulness of the project or anything :)

