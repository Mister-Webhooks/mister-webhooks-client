from dataclasses import dataclass
from enum import Enum, auto
from typing import Dict, List, Literal, Optional, Tuple, TypedDict
import avro.io
import avro.schema

__all__ = []

_envelope_schema_v1 = """{
  "type": "record",
  "name": "KafkaMessageEnvelopeV1",
  "namespace": "com.mister_webhooks.data",
  "fields": [
    {
      "name": "method",
      "type": {
        "type": "enum",
        "name": "Methods",
        "symbols": ["GET", "HEAD", "POST", "PUT", "DELETE", "PATCH"]
      }
    },
    {
      "name": "headers",
      "type": {
        "type": "map",
        "values": {
          "type": "array",
          "items": "string"
        }
      }
    },
    {
      "name": "payload",
      "type": "bytes"
    },
    {
      "name": "encoding",
      "type": {
        "type": "enum",
        "name": "Encodings",
        "symbols": ["JSON", "CBOR"]
      }
    }
  ]
}"""

envelope_reader = avro.io.DatumReader(avro.schema.parse(_envelope_schema_v1))


class Encoding(Enum):
    CBOR = auto
    JSON = auto


class KafkaMessageEnvelopeV1(TypedDict):
    encoding: Literal["JSON", "CBOR"]
    method: Literal["GET", "HEAD", "POST", "PUT", "DELETE", "PATCH"]
    headers: Dict[str, List[bytes]]
    payload: bytes


@dataclass
class ConsumerRecord:
    topic: str
    partition: int
    leader_epoch: int
    offset: int
    timestamp: int
    timestamp_type: int
    key: bytes
    value: bytes
    headers: List[Tuple[str, List[bytes]]]
    checksum: Optional[bytes]
    serialized_key_size: int
    serialized_value_size: int
    serialized_header_size: int
