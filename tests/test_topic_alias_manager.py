import struct

import pytest

from paho.mqtt.client import MQTTErrorCode, MQTTv5, Client, TopicAliasManager
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

import tests.mqtt5_props as mqtt5_props
import tests.paho_test as paho_test


class FakeSocket:
    def __init__(self):
        self.sent = bytearray()
        self.blocking = True
        self.closed = False

    def send(self, data):
        length = len(data)
        self.sent.extend(data)
        return length

    def close(self):
        self.closed = True

    def fileno(self):
        return -1

    def setblocking(self, flag):
        self.blocking = flag


def decode_remaining_length(data, offset=0):
    multiplier = 1
    value = 0
    while True:
        digit = data[offset]
        offset += 1
        value += (digit & 127) * multiplier
        if digit & 128 == 0:
            return value, offset
        multiplier *= 128


def decode_publish_packet(data, offset):
    command = data[offset]
    remaining_length, offset = decode_remaining_length(data, offset + 1)
    header_end = offset
    packet_end = header_end + remaining_length
    packet = data[offset:packet_end]
    position = 0

    topic_length = struct.unpack_from("!H", packet, position)[0]
    position += 2
    topic = bytes(packet[position:position + topic_length])
    position += topic_length

    qos = (command & 0x06) >> 1
    if qos > 0:
        position += 2

    properties_length, properties_end_of_length = decode_remaining_length(packet, position)
    position = properties_end_of_length
    properties_end = position + properties_length
    alias = None
    while position < properties_end:
        identifier, position = decode_remaining_length(packet, position)
        if identifier == 35:
            alias = struct.unpack_from("!H", packet, position)[0]
            position += 2
        else:
            pytest.fail(f"unexpected property {identifier}")

    return {
        "command": command,
        "topic": topic,
        "alias": alias,
        "payload": bytes(packet[properties_end:]),
        "end": packet_end,
    }


def decode_publish_packets(data):
    packets = []
    offset = 0
    while offset < len(data):
        if data[offset] & 0xF0 != 0x30:
            _remaining_length, offset = skip_packet(data, offset)
            continue
        packet = decode_publish_packet(data, offset)
        offset = packet.pop("end")
        packets.append(packet)
    return packets


def skip_packet(data, offset):
    remaining_length, header_end = decode_remaining_length(data, offset + 1)
    return remaining_length, header_end + remaining_length


def connack_body(topic_alias_maximum):
    properties = Properties(PacketTypes.CONNACK)
    properties.TopicAliasMaximum = topic_alias_maximum
    return b"\x00\x00" + properties.pack()


def handle_connack(client, topic_alias_maximum):
    body = connack_body(topic_alias_maximum)
    client._in_packet["remaining_length"] = len(body)
    client._in_packet["packet"] = bytearray(body)
    assert client._handle_connack() == MQTTErrorCode.MQTT_ERR_SUCCESS


def establish_connection(client, socket, topic_alias_maximum=1):
    client._sock = socket
    assert client._send_connect(60) == MQTTErrorCode.MQTT_ERR_SUCCESS
    handle_connack(client, topic_alias_maximum)
    socket.sent.clear()


def use_fake_socket(socket):
    def create_socket():
        return socket

    return create_socket


def test_automatic_outgoing_alias_limit_one_eviction_and_reconnect():
    client = Client(
        CallbackAPIVersion.VERSION2,
        "topic-alias-client",
        protocol=MQTTv5,
        reconnect_on_failure=False,
        topic_alias_manager=TopicAliasManager(),
    )
    client.connect_async("localhost", 1883)
    socket = FakeSocket()
    establish_connection(client, socket, 1)

    client.publish("long/topic/a", b"1")
    client.publish("long/topic/b", b"2")
    client.publish("long/topic/a", b"3")
    client.publish("long/topic/b", b"4")
    client.publish("long/topic/b", b"5")

    assert decode_publish_packets(socket.sent) == [
        {"command": 0x30, "topic": b"long/topic/a", "alias": 1, "payload": b"1"},
        {"command": 0x30, "topic": b"long/topic/b", "alias": 1, "payload": b"2"},
        {"command": 0x30, "topic": b"long/topic/a", "alias": 1, "payload": b"3"},
        {"command": 0x30, "topic": b"long/topic/b", "alias": 1, "payload": b"4"},
        {"command": 0x30, "topic": b"", "alias": 1, "payload": b"5"},
    ]

    new_socket = FakeSocket()
    client._create_socket = use_fake_socket(new_socket)
    assert client.reconnect() == MQTTErrorCode.MQTT_ERR_SUCCESS
    handle_connack(client, 1)
    new_socket.sent.clear()

    client.publish("long/topic/a", b"after-reconnect")

    assert decode_publish_packets(new_socket.sent) == [
        {"command": 0x30, "topic": b"long/topic/a", "alias": 1, "payload": b"after-reconnect"},
    ]


def test_explicit_alias_conflicts_are_bidirectional():
    manager = TopicAliasManager()
    manager.set_outgoing_maximum(2)
    first_properties = Properties(PacketTypes.PUBLISH)
    first_properties.TopicAlias = 1

    topic, properties = manager.resolve_outgoing(b"topic/a", first_properties)
    assert topic == b"topic/a"
    assert properties.TopicAlias == 1

    topic, properties = manager.resolve_outgoing(b"topic/a", first_properties)
    assert topic == b""
    assert properties.TopicAlias == 1
    assert first_properties is not properties

    topic, properties = manager.resolve_outgoing(b"topic/b", None)
    assert topic == b"topic/b"
    assert properties.TopicAlias == 2

    replacement_properties = Properties(PacketTypes.PUBLISH)
    replacement_properties.TopicAlias = 1
    topic, properties = manager.resolve_outgoing(b"topic/b", replacement_properties)
    assert topic == b"topic/b"
    assert properties.TopicAlias == 1

    topic, properties = manager.resolve_outgoing(b"topic/b", replacement_properties)
    assert topic == b""
    assert properties.TopicAlias == 1

    topic, properties = manager.resolve_outgoing(b"", first_properties)
    assert topic == b""
    assert properties.TopicAlias == 1


def test_explicit_alias_respects_broker_limit_and_empty_topic():
    manager = TopicAliasManager()
    manager.set_outgoing_maximum(1)
    properties = Properties(PacketTypes.PUBLISH)
    properties.TopicAlias = 2

    with pytest.raises(ValueError):
        manager.resolve_outgoing(b"topic/a", properties)

    with pytest.raises(ValueError):
        manager.resolve_outgoing(b"", None)

    manager.set_outgoing_maximum(0)
    with pytest.raises(ValueError):
        manager.resolve_outgoing(b"topic/a", properties)


def test_automatic_alias_disabled_when_broker_limit_is_zero():
    manager = TopicAliasManager()
    manager.set_outgoing_maximum(0)

    topic, properties = manager.resolve_outgoing(b"topic/a", None)

    assert topic == b"topic/a"
    assert properties is None


def test_connection_configuration_sets_independent_incoming_limit():
    manager = TopicAliasManager()

    incoming_maximum, connect_properties = manager.configure_connection(None)

    assert incoming_maximum == 65535
    assert connect_properties.TopicAliasMaximum == 65535

    explicit_properties = Properties(PacketTypes.CONNECT)
    explicit_properties.TopicAliasMaximum = 1
    incoming_maximum, configured_properties = manager.configure_connection(explicit_properties)

    assert incoming_maximum == 1
    assert configured_properties is explicit_properties


def test_incoming_aliases_are_independent_and_connection_scoped():
    manager = TopicAliasManager()
    connect_properties = Properties(PacketTypes.CONNECT)
    connect_properties.TopicAliasMaximum = 1
    manager.configure_connection(connect_properties)
    manager.set_outgoing_maximum(1)

    outgoing_topic, outgoing_properties = manager.resolve_outgoing(b"outgoing/topic", None)
    assert outgoing_topic == b"outgoing/topic"
    assert manager.resolve_incoming(b"", 1) is None

    first_packet = paho_test.gen_publish(
        "incoming/topic",
        0,
        b"first",
        proto_ver=5,
        properties=mqtt5_props.gen_uint16_prop(mqtt5_props.PROP_TOPIC_ALIAS, 1),
    )
    second_packet = paho_test.gen_publish(
        "",
        0,
        b"second",
        proto_ver=5,
        properties=mqtt5_props.gen_uint16_prop(mqtt5_props.PROP_TOPIC_ALIAS, 1),
    )

    received = []
    client = Client(
        CallbackAPIVersion.VERSION2,
        "incoming-topic-alias-client",
        protocol=MQTTv5,
        reconnect_on_failure=False,
        topic_alias_manager=manager,
    )
    client.on_message = lambda _client, _userdata, message: received.append(message)

    for packet in (first_packet, second_packet):
        body = packet[2:] if packet[1] < 128 else packet[3:]
        client._in_packet["command"] = packet[0]
        client._in_packet["remaining_length"] = len(body)
        client._in_packet["packet"] = bytearray(body)
        assert client._handle_publish() == MQTTErrorCode.MQTT_ERR_SUCCESS

    assert [message.topic for message in received] == ["incoming/topic", "incoming/topic"]
    assert manager.resolve_incoming(b"", 1) == b"incoming/topic"

    manager.reset()
    assert manager.resolve_incoming(b"", 1) is None


def test_invalid_incoming_alias_is_a_protocol_error():
    connect_properties = Properties(PacketTypes.CONNECT)
    connect_properties.TopicAliasMaximum = 0
    manager = TopicAliasManager()
    manager.configure_connection(connect_properties)

    client = Client(
        CallbackAPIVersion.VERSION2,
        "invalid-incoming-topic-alias-client",
        protocol=MQTTv5,
        reconnect_on_failure=False,
        topic_alias_manager=manager,
    )
    packet = paho_test.gen_publish(
        "topic/a",
        0,
        b"payload",
        proto_ver=5,
        properties=mqtt5_props.gen_uint16_prop(mqtt5_props.PROP_TOPIC_ALIAS, 1),
    )
    body = packet[2:]
    client._in_packet["command"] = packet[0]
    client._in_packet["remaining_length"] = len(body)
    client._in_packet["packet"] = bytearray(body)

    assert client._handle_publish() == MQTTErrorCode.MQTT_ERR_PROTOCOL
