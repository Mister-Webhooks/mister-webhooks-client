from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
import io
import json
from typing import Any, ByteString, Dict, List

import avro.io
import cbor2

from .types import (
    KafkaMessageEnvelopeV1,
    KafkaMessageEnvelopeV1,
    envelope_reader,
)

__all__ = ["WebhookEvent", "HTTPMethod"]


class DecodeError(ValueError):
    pass


class EnvelopeDecodeError(DecodeError):
    pass


class PayloadDecodeError(DecodeError):
    pass


class HTTPMethod(Enum):
    """HTTPMethod represents the HTTP method used to transmit the webhook event.

    Only the document manipulation methods are encoded here, namely:

    - GET
    - HEAD
    - POST
    - PUT
    - PATCH
    - DELETE

    """

    GET = auto
    HEAD = auto
    POST = auto
    PUT = auto
    DELETE = auto
    PATCH = auto


@dataclass
class WebhookEvent:
    "WebhookEvent represents a received webhook event"

    method: HTTPMethod
    """HTTP method used to transmit the webhook event"""

    timestamp: datetime
    """Time at which the webhook event occurred"""

    headers: Dict[str, List[bytes]]
    """HTTP headers captured from the webhook request"""

    payload: Any
    """Webhook request body"""

    @staticmethod
    def decode(
        envelopeType: int, timestamp: datetime, raw: ByteString
    ) -> "WebhookEvent":
        match envelopeType:
            case 0x80:
                try:
                    envelope: KafkaMessageEnvelopeV1 = envelope_reader.read(
                        avro.io.BinaryDecoder(io.BytesIO(raw))
                    )  # type: ignore
                except Exception as exc:
                    raise EnvelopeDecodeError(exc)

                try:
                    return WebhookEvent._from_kafka_message_envelope_v1(
                        timestamp, envelope
                    )
                except Exception as exc:
                    raise PayloadDecodeError(exc)

            case unknown:
                raise ValueError(
                    "envelope type 0x%2x is unsupported, please upgrade mister-webhooks-client"
                    % unknown
                )

    @staticmethod
    def _from_kafka_message_envelope_v1(
        timestamp: datetime, envelope: KafkaMessageEnvelopeV1
    ) -> "WebhookEvent":
        match envelope["encoding"]:
            case "CBOR":
                body = cbor2.loads(envelope["payload"])
            case "JSON":
                body = json.loads(envelope["payload"])

        return WebhookEvent(
            HTTPMethod[envelope["method"]],
            timestamp,
            envelope["headers"],
            body,
        )
