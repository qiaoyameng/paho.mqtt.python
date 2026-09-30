import struct

import pytest

from paho.mqtt import client as mqtt_client
from paho.mqtt.client import (
    CallbackAPIVersion,
    MQTTErrorCode,
    MQTTMessage,
    MQTTv5,
    SubscriptionHandle,
)
from paho.mqtt.enums import _ConnectionState
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.subscribeoptions import SubscribeOptions


def make_client():
    return mqtt_client.Client(CallbackAPIVersion.VERSION2, protocol=MQTTv5)


class CloseableSocket:
    def close(self):
        pass


def make_handle(client, subscription_identifier):
    calls = []

    def callback(received_client, userdata, message):
        calls.append((subscription_identifier, message.topic))

    handle = SubscriptionHandle(client, subscription_identifier, callback)
    client._subscription_handles[subscription_identifier] = handle
    return handle, calls


def suback_packet(mid, reason_codes):
    return struct.pack("!HB" + "B" * len(reason_codes), mid, 0, *reason_codes)


def unsuback_packet(mid, reason_codes):
    return struct.pack("!HB" + "B" * len(reason_codes), mid, 0, *reason_codes)


def publish_message(subscription_identifiers):
    properties = Properties(PacketTypes.PUBLISH)
    for subscription_identifier in subscription_identifiers:
        properties.SubscriptionIdentifier = subscription_identifier
    message = MQTTMessage()
    message.topic = b"sensor/temp"
    message.properties = properties
    return message


class TestSubscriptionHandles:
    def test_overlapping_wildcards_and_duplicate_identifiers(self):
        mqttc = make_client()
        first_handle, first_calls = make_handle(mqttc, 1)
        second_handle, second_calls = make_handle(mqttc, 2)

        mqttc._subscription_epoch = 1
        mqttc._pending_subscriptions[10] = {
            "epoch": 1,
            "filters": ["sensor/+", "sensor/#"],
            "handle": first_handle,
        }
        mqttc._pending_subscriptions[11] = {
            "epoch": 1,
            "filters": ["#"],
            "handle": second_handle,
        }

        mqttc._in_packet.update(
            command=PacketTypes.SUBACK << 4,
            remaining_length=len(suback_packet(10, [0, 1])),
            packet=bytearray(suback_packet(10, [0, 1])),
        )
        mqttc._handle_suback()
        mqttc._in_packet.update(
            command=PacketTypes.SUBACK << 4,
            remaining_length=len(suback_packet(11, [0])),
            packet=bytearray(suback_packet(11, [0])),
        )
        mqttc._handle_suback()

        assert first_handle.active_topics == {"sensor/+", "sensor/#"}
        assert second_handle.active_topics == {"#"}

        mqttc._handle_on_message(publish_message([1, 1, 2]))

        assert first_calls == [(1, "sensor/temp")]
        assert second_calls == [(2, "sensor/temp")]

    def test_partial_suback_rejection_activates_only_successful_filters(self):
        mqttc = make_client()
        handle, calls = make_handle(mqttc, 7)
        mqttc._subscription_epoch = 2
        mqttc._pending_subscriptions[20] = {
            "epoch": 2,
            "filters": ["allowed/#", "forbidden/#"],
            "handle": handle,
        }

        packet = suback_packet(20, [0, 0x87])
        mqttc._in_packet.update(
            command=PacketTypes.SUBACK << 4,
            remaining_length=len(packet),
            packet=bytearray(packet),
        )
        mqttc._handle_suback()

        assert handle.active_topics == {"allowed/#"}
        assert 20 not in mqttc._pending_subscriptions

    def test_message_before_suback_does_not_invoke_handle(self):
        mqttc = make_client()
        handle, calls = make_handle(mqttc, 3)
        mqttc._subscription_epoch = 1
        mqttc._pending_subscriptions[30] = {
            "epoch": 1,
            "filters": ["sensor/#"],
            "handle": handle,
        }

        mqttc._handle_on_message(publish_message([3]))

        assert calls == []
        assert not handle.active

    def test_malformed_suback_does_not_activate_or_consume_pending(self):
        mqttc = make_client()
        handle, _ = make_handle(mqttc, 3)
        mqttc._subscription_epoch = 1
        mqttc._pending_subscriptions[31] = {
            "epoch": 1,
            "filters": ["a/#", "b/#"],
            "handle": handle,
        }

        packet = suback_packet(31, [0])
        mqttc._in_packet.update(
            command=PacketTypes.SUBACK << 4,
            remaining_length=len(packet),
            packet=bytearray(packet),
        )
        mqttc._handle_suback()

        assert not handle.active
        assert mqttc._pending_subscriptions[31]["handle"] is handle

    def test_unsuback_revokes_only_successful_filters(self):
        mqttc = make_client()
        handle, calls = make_handle(mqttc, 4)
        handle._add_topic("a/#")
        handle._add_topic("b/#")
        mqttc._subscription_epoch = 1
        mqttc._pending_unsubscriptions[40] = {
            "epoch": 1,
            "filters": ["a/#", "b/#"],
            "handle": handle,
        }

        packet = unsuback_packet(40, [0, 0x80])
        mqttc._in_packet.update(
            command=PacketTypes.UNSUBACK << 4,
            remaining_length=len(packet),
            packet=bytearray(packet),
        )
        assert mqttc._handle_unsuback() == MQTTErrorCode.MQTT_ERR_SUCCESS

        assert handle.active_topics == {"b/#"}
        assert 40 not in mqttc._pending_unsubscriptions

        mqttc._handle_on_message(publish_message([4]))
        assert calls == []

    def test_malformed_unsuback_does_not_revoke_or_consume_pending(self):
        mqttc = make_client()
        handle, _ = make_handle(mqttc, 4)
        handle._add_topic("a/#")
        handle._add_topic("b/#")
        mqttc._subscription_epoch = 1
        mqttc._pending_unsubscriptions[41] = {
            "epoch": 1,
            "filters": ["a/#", "b/#"],
            "handle": handle,
        }

        packet = unsuback_packet(41, [0])
        mqttc._in_packet.update(
            command=PacketTypes.UNSUBACK << 4,
            remaining_length=len(packet),
            packet=bytearray(packet),
        )
        assert mqttc._handle_unsuback() == MQTTErrorCode.MQTT_ERR_SUCCESS

        assert handle.active_topics == {"a/#", "b/#"}
        assert mqttc._pending_unsubscriptions[41]["handle"] is handle

    def test_old_suback_does_not_activate_reconnected_subscription(self):
        mqttc = make_client()
        handle, calls = make_handle(mqttc, 5)
        mqttc._subscription_epoch = 1
        mqttc._pending_subscriptions[50] = {
            "epoch": 1,
            "filters": ["old/#"],
            "handle": handle,
        }
        mqttc._subscription_epoch = 2

        packet = suback_packet(50, [0])
        mqttc._in_packet.update(
            command=PacketTypes.SUBACK << 4,
            remaining_length=len(packet),
            packet=bytearray(packet),
        )
        mqttc._handle_suback()

        assert not handle.active
        assert 50 not in mqttc._pending_subscriptions

    def test_subscription_identifiers_require_mqtt5(self):
        mqttc = mqtt_client.Client(CallbackAPIVersion.VERSION2)
        handle, _ = make_handle(mqttc, 1)

        with pytest.raises(ValueError, match="MQTT version 5"):
            mqttc.subscribe_with_id(1, "topic", handle._callback)

    def test_subscribe_with_id_creates_pending_transaction(self):
        mqttc = make_client()
        mqttc._sock = CloseableSocket()

        def callback(received_client, userdata, message):
            pass

        rc, mid, handle = mqttc.subscribe_with_id(
            42,
            [("sensor/+", SubscribeOptions(qos=1)), ("sensor/#", SubscribeOptions(qos=2))],
            callback,
        )

        assert rc == MQTTErrorCode.MQTT_ERR_SUCCESS
        assert mid is not None
        assert isinstance(handle, SubscriptionHandle)
        assert not handle.active
        assert mqttc._pending_subscriptions[mid] == {
            "epoch": 0,
            "filters": ["sensor/+", "sensor/#"],
            "handle": handle,
        }

    def test_reuse_identifier_requires_same_callback(self):
        mqttc = make_client()
        mqttc._sock = CloseableSocket()

        mqttc.subscribe_with_id(43, "topic", lambda c, u, m: None)

        with pytest.raises(ValueError, match="already assigned"):
            mqttc.subscribe_with_id(43, "other", lambda c, u, m: None)

    def test_identifier_topic_and_default_callbacks_are_ordered(self):
        mqttc = make_client()
        handle, identifier_calls = make_handle(mqttc, 8)
        handle._add_topic("sensor/#")
        calls = []

        def topic_callback(received_client, userdata, message):
            calls.append("topic")

        def default_callback(received_client, userdata, message):
            calls.append("default")

        mqttc.message_callback_add("sensor/+", topic_callback)
        mqttc.on_message = default_callback
        mqttc._handle_on_message(publish_message([8]))

        assert identifier_calls == [(8, "sensor/temp")]
        assert calls == ["topic"]

    def test_identifier_and_default_callbacks_are_ordered_without_topic_callback(self):
        mqttc = make_client()
        handle, identifier_calls = make_handle(mqttc, 9)
        handle._add_topic("sensor/#")
        calls = []

        def default_callback(received_client, userdata, message):
            calls.append("default")

        mqttc.on_message = default_callback
        mqttc._handle_on_message(publish_message([9]))

        assert identifier_calls == [(9, "sensor/temp")]
        assert calls == ["default"]

    def test_successful_connack_clears_pending_acknowledgements(self):
        mqttc = make_client()
        handle, _ = make_handle(mqttc, 6)
        handle._add_topic("clean/#")
        mqttc._pending_subscriptions[60] = {
            "epoch": 1,
            "filters": ["clean/#"],
            "handle": handle,
        }
        mqttc._pending_unsubscriptions[61] = {
            "epoch": 1,
            "filters": ["clean/#"],
            "handle": handle,
        }
        mqttc._state = _ConnectionState.MQTT_CS_CONNECTING
        mqttc._in_packet.update(
            command=PacketTypes.CONNACK << 4,
            remaining_length=3,
            packet=bytearray(struct.pack("!BBB", 0, 0, 0)),
        )

        assert mqttc._handle_connack() == MQTTErrorCode.MQTT_ERR_SUCCESS

        assert not handle.active
        assert not mqttc._pending_subscriptions
        assert not mqttc._pending_unsubscriptions
        assert mqttc._subscription_epoch == 1

    def test_session_present_preserves_active_subscriptions_but_invalidates_old_acks(self):
        mqttc = make_client()
        handle, calls = make_handle(mqttc, 6)
        handle._add_topic("sensor/#")
        mqttc._subscription_epoch = 1
        mqttc._pending_subscriptions[62] = {
            "epoch": 1,
            "filters": ["pending/#"],
            "handle": handle,
        }
        mqttc._state = _ConnectionState.MQTT_CS_CONNECTING
        mqttc._in_packet.update(
            command=PacketTypes.CONNACK << 4,
            remaining_length=3,
            packet=bytearray(struct.pack("!BBB", 1, 0, 0)),
        )

        assert mqttc._handle_connack() == MQTTErrorCode.MQTT_ERR_SUCCESS

        assert handle.active_topics == {"sensor/#"}
        assert 62 not in mqttc._pending_subscriptions

        packet = suback_packet(62, [0])
        mqttc._in_packet.update(
            command=PacketTypes.SUBACK << 4,
            remaining_length=len(packet),
            packet=bytearray(packet),
        )
        mqttc._handle_suback()

        assert handle.active_topics == {"sensor/#"}
        mqttc._handle_on_message(publish_message([6]))
        assert calls == [(6, "sensor/temp")]
