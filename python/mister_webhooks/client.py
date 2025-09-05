from datetime import datetime
from typing import (
    Callable,
    Dict,
)
from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition
from .connection_profile import ConnectionProfile
from .types import (
    ConsumerRecord,
)
from .webhook_event import WebhookEvent
from .ssl_cert import new_ssl_client_context

__all__ = ["FormatError", "MisterWebhooksConsumer"]


class FormatError(ValueError):
    pass


def decode_message(raw: ConsumerRecord) -> None | WebhookEvent:
    envelopeType: int = -1

    for name, val in raw.headers:
        if name == "envelope":
            if len(val) != 1:
                raise FormatError(
                    "malformed Kafka message, 'envelope' header value must be single byte"
                )
            envelopeType = int(val[0])
            break
    else:
        raise FormatError("malformed Kafka message, 'envelope' header is missing")

    if envelopeType < 0x00:
        raise ValueError("impossible: envelope type is negative")

    if envelopeType < 0x80:
        return None

    return WebhookEvent.decode(
        envelopeType, datetime.fromtimestamp(raw.timestamp / 1000), raw.value
    )


class MisterWebhooksConsumer:
    """MisterWebhooksConsumer is a message consumer for Mister Webhooks events.

    Parameters:
        topic (str): The Mister Webhooks topic to consume from.
        profile (ConnectionProfile): The connection profile to connect with.
    """

    kafkaConsumer: KafkaConsumer

    def __init__(self, topic: str, profile: ConnectionProfile) -> None:
        auth = {}

        match profile.auth_mechanism:
            case "plain":
                auth: Dict[str, str] = {
                    "sasl_mechanism": "PLAIN",
                    "sasl_plain_username": profile.consumer_name,
                    "sasl_plain_password": profile.auth_secret,
                }

        self.kafkaConsumer = KafkaConsumer(
            topic,
            group_id=profile.consumer_name,
            bootstrap_servers=[profile.kafka_bootstrap],
            enable_auto_commit=False,
            allow_auto_create_topics=False,
            security_protocol="SASL_SSL",
            ssl_context=new_ssl_client_context(),
            auto_offset_reset="earliest",
            **auth,
        )

    def run(self, cb: Callable[[WebhookEvent], None]) -> None:
        """
        Run runs the consumer and passes each incoming webhook event to the provided callback. It automatically
        takes care of offset bookkeeping when the callback runs without error. All exceptions thrown by the
        callback are re-raised.

        Parameters:
            cb (Callable[[WebhookEvent], None]): The callback to run on each message.
        """
        for msg in self.kafkaConsumer:
            decoded: None | WebhookEvent = decode_message(msg)
            match decoded:
                case None:
                    continue
                case x:
                    cb(x)
                    self.kafkaConsumer.commit(
                        offsets=MisterWebhooksConsumer.__messageCommitOffset(msg)
                    )

    @staticmethod
    def __messageCommitOffset(
        msg: ConsumerRecord,
    ) -> Dict[TopicPartition, OffsetAndMetadata]:
        return {
            TopicPartition(topic=msg.topic, partition=msg.partition): OffsetAndMetadata(
                offset=msg.offset + 1, metadata=None, leader_epoch=msg.leader_epoch
            )
        }
