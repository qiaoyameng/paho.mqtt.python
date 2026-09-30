import threading

import paho.mqtt.client as client
import pytest
from paho.mqtt.enums import CallbackAPIVersion, MQTTProtocolVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.topicalias import TopicAliasManager

import tests.mqtt5_props as mqtt5_props
import tests.paho_test as paho_test

# Import test fixture
from tests.testsupport.broker import FakeBroker, fake_broker  # noqa: F401


def alias_prop(alias):
    return mqtt5_props.gen_uint16_prop(mqtt5_props.PROP_TOPIC_ALIAS, alias)


def gen_connack_alias_maximum(maximum):
    return paho_test.gen_connack(
        rc=0,
        proto_ver=5,
        properties=mqtt5_props.gen_uint16_prop(mqtt5_props.PROP_TOPIC_ALIAS_MAXIMUM, maximum),
        property_helper=False,
    )


def gen_publish_alias(topic, payload, alias):
    return paho_test.gen_publish(
        topic, qos=0, payload=payload, proto_ver=5, properties=alias_prop(alias)
    )


class TestTopicAliasManager:
    def test_maximum_must_be_valid(self):
        with pytest.raises(ValueError):
            TopicAliasManager(0)
        with pytest.raises(ValueError):
            TopicAliasManager(65536)

    def test_first_publish_sends_full_topic_second_omits_it(self):
        mgr = TopicAliasManager(2)
        mgr.reset(broker_maximum=10)

        topic, props = mgr.apply(b"a/long/topic", None)
        assert topic == b"a/long/topic"
        assert props.TopicAlias == 1

        topic, props = mgr.apply(b"a/long/topic", None)
        assert topic == b""
        assert props.TopicAlias == 1

    def test_broker_maximum_is_respected(self):
        mgr = TopicAliasManager(10)
        mgr.reset(broker_maximum=1)
        assert mgr.maximum == 1

        # Only a single alias is usable, so alternating topics always
        # re-send the full topic.
        assert mgr.apply(b"topic/a", None)[0] == b"topic/a"
        assert mgr.apply(b"topic/a", None)[0] == b""
        assert mgr.apply(b"topic/b", None)[0] == b"topic/b"
        assert mgr.apply(b"topic/a", None)[0] == b"topic/a"

    def test_no_alias_when_broker_disallows(self):
        mgr = TopicAliasManager(10)
        mgr.reset(broker_maximum=0)
        assert mgr.maximum == 0

        topic, props = mgr.apply(b"topic/a", None)
        assert topic == b"topic/a"
        assert props is None

    def test_alias_replacement_resends_topic(self):
        mgr = TopicAliasManager(2)
        mgr.reset(broker_maximum=2)

        assert mgr.apply(b"topic/a", None)[1].TopicAlias == 1
        assert mgr.apply(b"topic/b", None)[1].TopicAlias == 2
        # All aliases in use: the least recently used one (topic/a) is
        # reassigned and the full topic is sent again.
        topic, props = mgr.apply(b"topic/c", None)
        assert topic == b"topic/c"
        assert props.TopicAlias == 1
        # topic/a no longer has an alias.
        topic, props = mgr.apply(b"topic/a", None)
        assert topic == b"topic/a"
        assert props.TopicAlias == 2

    def test_reset_clears_mappings(self):
        mgr = TopicAliasManager(2)
        mgr.reset(broker_maximum=2)
        assert mgr.apply(b"topic/a", None)[0] == b"topic/a"
        assert mgr.apply(b"topic/a", None)[0] == b""

        # A new network connection: aliases are no longer valid.
        mgr.reset(broker_maximum=2)
        assert mgr.apply(b"topic/a", None)[0] == b"topic/a"

    def test_explicit_alias_is_used_and_adopted(self):
        mgr = TopicAliasManager(2)
        mgr.reset(broker_maximum=2)

        user_props = Properties(PacketTypes.PUBLISH)
        user_props.TopicAlias = 2
        topic, props = mgr.apply(b"topic/x", user_props)
        # The full topic is always sent with an explicit alias and the
        # caller's properties are passed through untouched.
        assert topic == b"topic/x"
        assert props is user_props

        # The manager adopted the explicit mapping.
        topic, props = mgr.apply(b"topic/x", None)
        assert topic == b""
        assert props.TopicAlias == 2

    def test_explicit_alias_conflict_unbinds_previous_topic(self):
        mgr = TopicAliasManager(2)
        mgr.reset(broker_maximum=2)

        assert mgr.apply(b"topic/auto", None)[1].TopicAlias == 1

        # Explicitly assigning alias 1 to another topic wins: the previous
        # automatic mapping for alias 1 is dropped.
        user_props = Properties(PacketTypes.PUBLISH)
        user_props.TopicAlias = 1
        topic, _ = mgr.apply(b"topic/manual", user_props)
        assert topic == b"topic/manual"

        topic, props = mgr.apply(b"topic/auto", None)
        assert topic == b"topic/auto"
        assert props.TopicAlias == 2

        topic, props = mgr.apply(b"topic/manual", None)
        assert topic == b""
        assert props.TopicAlias == 1

    def test_explicit_alias_out_of_range_raises(self):
        mgr = TopicAliasManager(2)
        mgr.reset(broker_maximum=1)

        user_props = Properties(PacketTypes.PUBLISH)
        user_props.TopicAlias = 2
        with pytest.raises(ValueError):
            mgr.apply(b"topic/x", user_props)

        # Broker does not allow aliases at all.
        mgr.reset(broker_maximum=0)
        user_props = Properties(PacketTypes.PUBLISH)
        user_props.TopicAlias = 1
        with pytest.raises(ValueError):
            mgr.apply(b"topic/x", user_props)

    def test_caller_properties_are_never_modified(self):
        mgr = TopicAliasManager(2)
        mgr.reset(broker_maximum=2)

        user_props = Properties(PacketTypes.PUBLISH)
        user_props.MessageExpiryInterval = 60
        topic, props = mgr.apply(b"topic/a", user_props)

        assert not hasattr(user_props, "TopicAlias")
        assert props is not user_props
        assert props.TopicAlias == 1
        assert props.MessageExpiryInterval == 60


class TestTopicAliasPublish:
    def test_interleaved_topics_and_reconnect(self, fake_broker: FakeBroker) -> None:
        client_id = "topic-alias-interleave"
        mqttc = client.Client(
            CallbackAPIVersion.VERSION2,
            client_id,
            protocol=MQTTProtocolVersion.MQTTv5,
            transport=fake_broker.transport,
        )
        mqttc.enable_topic_alias(10)

        is_connected = threading.Event()

        def on_connect(mqttc, obj, flags, reason, properties):
            assert reason == 0
            is_connected.set()

        mqttc.on_connect = on_connect

        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        connect_packet = paho_test.gen_connect(
            client_id, keepalive=60, proto_ver=5, properties=None
        )
        # clean_start is only set on the first connect
        reconnect_packet = paho_test.gen_connect(
            client_id, keepalive=60, clean_session=False, proto_ver=5, properties=None
        )

        try:
            fake_broker.start()
            fake_broker.expect_packet("connect", connect_packet)

            # The broker only allows a single topic alias.
            count = fake_broker.send_packet(gen_connack_alias_maximum(1))
            assert count > 0
            assert is_connected.wait(5)

            # First use of a topic: full topic name and alias.
            mqttc.publish("topic/one", "m1")
            fake_broker.expect_packet("publish", gen_publish_alias("topic/one", "m1", 1))

            # Second use of the same topic: topic name omitted.
            mqttc.publish("topic/one", "m2")
            fake_broker.expect_packet("publish", gen_publish_alias(b"", "m2", 1))

            # Interleaving another topic replaces the only alias, so the
            # full topic name must be sent again.
            mqttc.publish("topic/two", "m3")
            fake_broker.expect_packet("publish", gen_publish_alias("topic/two", "m3", 1))
            mqttc.publish("topic/one", "m4")
            fake_broker.expect_packet("publish", gen_publish_alias("topic/one", "m4", 1))

            # Aliases are only valid for the current network connection.
            is_connected.clear()
            mqttc.disconnect()
            fake_broker.expect_packet("disconnect", paho_test.gen_disconnect())

            mqttc.reconnect()
            fake_broker.start()
            fake_broker.expect_packet("reconnect", reconnect_packet)
            count = fake_broker.send_packet(gen_connack_alias_maximum(1))
            assert count > 0
            assert is_connected.wait(5)

            # After the reconnect the client must not send an empty topic.
            mqttc.publish("topic/one", "m5")
            fake_broker.expect_packet("publish", gen_publish_alias("topic/one", "m5", 1))
            mqttc.publish("topic/one", "m6")
            fake_broker.expect_packet("publish", gen_publish_alias(b"", "m6", 1))

            mqttc.disconnect()
            fake_broker.expect_packet("disconnect", paho_test.gen_disconnect())
        finally:
            mqttc.loop_stop()

        packet_in = fake_broker.receive_packet(1)
        assert not packet_in  # Check connection is closed

    def test_explicit_alias_conflict_rules(self, fake_broker: FakeBroker) -> None:
        mqttc = client.Client(
            CallbackAPIVersion.VERSION2,
            "topic-alias-explicit",
            protocol=MQTTProtocolVersion.MQTTv5,
            transport=fake_broker.transport,
        )
        mqttc.enable_topic_alias(2)

        is_connected = threading.Event()

        def on_connect(mqttc, obj, flags, reason, properties):
            assert reason == 0
            is_connected.set()

        mqttc.on_connect = on_connect

        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()
            connect_packet = paho_test.gen_connect(
                "topic-alias-explicit", keepalive=60, proto_ver=5, properties=None
            )
            fake_broker.expect_packet("connect", connect_packet)

            # Broker allows 10 aliases, the client is configured for 2.
            count = fake_broker.send_packet(gen_connack_alias_maximum(10))
            assert count > 0
            assert is_connected.wait(5)

            mqttc.publish("topic/auto", "m1")
            fake_broker.expect_packet("publish", gen_publish_alias("topic/auto", "m1", 1))

            # An explicit alias always wins and is sent with the full topic.
            user_props = Properties(PacketTypes.PUBLISH)
            user_props.TopicAlias = 1
            mqttc.publish("topic/manual", "m2", properties=user_props)
            fake_broker.expect_packet("publish", gen_publish_alias("topic/manual", "m2", 1))

            # topic/auto lost alias 1 and is assigned the next free alias.
            mqttc.publish("topic/auto", "m3")
            fake_broker.expect_packet("publish", gen_publish_alias("topic/auto", "m3", 2))

            # topic/manual keeps the explicitly assigned alias.
            mqttc.publish("topic/manual", "m4")
            fake_broker.expect_packet("publish", gen_publish_alias(b"", "m4", 1))

            # An explicit alias beyond the usable maximum is rejected.
            bad_props = Properties(PacketTypes.PUBLISH)
            bad_props.TopicAlias = 3
            with pytest.raises(ValueError):
                mqttc.publish("topic/x", "m5", properties=bad_props)

            # The caller's properties object is never modified.
            reused_props = Properties(PacketTypes.PUBLISH)
            mqttc.publish("topic/auto", "m6", properties=reused_props)
            fake_broker.expect_packet("publish", gen_publish_alias(b"", "m6", 2))
            assert not hasattr(reused_props, "TopicAlias")

            mqttc.disconnect()
            fake_broker.expect_packet("disconnect", paho_test.gen_disconnect())
        finally:
            mqttc.loop_stop()

        packet_in = fake_broker.receive_packet(1)
        assert not packet_in  # Check connection is closed


class TestTopicAliasReceive:
    def test_incoming_alias_is_resolved(self, fake_broker: FakeBroker) -> None:
        mqttc = client.Client(
            CallbackAPIVersion.VERSION2,
            "topic-alias-recv",
            protocol=MQTTProtocolVersion.MQTTv5,
            transport=fake_broker.transport,
        )

        is_connected = threading.Event()
        received = threading.Event()
        messages = []

        def on_connect(mqttc, obj, flags, reason, properties):
            assert reason == 0
            is_connected.set()

        def on_message(mqttc, obj, msg):
            messages.append(msg)
            received.set()

        mqttc.on_connect = on_connect
        mqttc.on_message = on_message

        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()
            connect_packet = paho_test.gen_connect(
                "topic-alias-recv", keepalive=60, proto_ver=5, properties=None
            )
            fake_broker.expect_packet("connect", connect_packet)
            count = fake_broker.send_packet(paho_test.gen_connack(rc=0, proto_ver=5))
            assert count > 0
            assert is_connected.wait(5)

            # The broker binds alias 3 to a topic...
            count = fake_broker.send_packet(gen_publish_alias("incoming/topic", "first", 3))
            assert count > 0
            assert received.wait(5)
            assert messages[-1].topic == "incoming/topic"
            assert messages[-1].payload == b"first"

            # ...then reuses the alias with an empty topic name.
            received.clear()
            count = fake_broker.send_packet(gen_publish_alias(b"", "second", 3))
            assert count > 0
            assert received.wait(5)
            assert messages[-1].topic == "incoming/topic"
            assert messages[-1].payload == b"second"

            # Rebinding the alias to another topic takes effect immediately.
            received.clear()
            count = fake_broker.send_packet(gen_publish_alias("other/topic", "third", 3))
            assert count > 0
            assert received.wait(5)
            assert messages[-1].topic == "other/topic"

            mqttc.disconnect()
            fake_broker.expect_packet("disconnect", paho_test.gen_disconnect())
        finally:
            mqttc.loop_stop()

        packet_in = fake_broker.receive_packet(1)
        assert not packet_in  # Check connection is closed

    def test_incoming_alias_reset_on_reconnect(self, fake_broker: FakeBroker) -> None:
        client_id = "topic-alias-recv-reconnect"
        mqttc = client.Client(
            CallbackAPIVersion.VERSION2,
            client_id,
            protocol=MQTTProtocolVersion.MQTTv5,
            transport=fake_broker.transport,
        )

        is_connected = threading.Event()
        received = threading.Event()
        messages = []

        def on_connect(mqttc, obj, flags, reason, properties):
            assert reason == 0
            is_connected.set()

        def on_message(mqttc, obj, msg):
            messages.append(msg)
            received.set()

        mqttc.on_connect = on_connect
        mqttc.on_message = on_message

        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        connect_packet = paho_test.gen_connect(
            client_id, keepalive=60, proto_ver=5, properties=None
        )
        reconnect_packet = paho_test.gen_connect(
            client_id, keepalive=60, clean_session=False, proto_ver=5, properties=None
        )

        try:
            fake_broker.start()
            fake_broker.expect_packet("connect", connect_packet)
            count = fake_broker.send_packet(paho_test.gen_connack(rc=0, proto_ver=5))
            assert count > 0
            assert is_connected.wait(5)

            count = fake_broker.send_packet(gen_publish_alias("incoming/topic", "first", 3))
            assert count > 0
            assert received.wait(5)
            assert messages[-1].topic == "incoming/topic"

            # Aliases from the previous connection must be forgotten.
            is_connected.clear()
            mqttc.disconnect()
            fake_broker.expect_packet("disconnect", paho_test.gen_disconnect())

            mqttc.reconnect()
            fake_broker.start()
            fake_broker.expect_packet("reconnect", reconnect_packet)
            count = fake_broker.send_packet(paho_test.gen_connack(rc=0, proto_ver=5))
            assert count > 0
            assert is_connected.wait(5)

            # The alias is unknown on the new connection: using it with an
            # empty topic is a protocol error and the client disconnects.
            count = fake_broker.send_packet(gen_publish_alias(b"", "second", 3))
            assert count > 0
        finally:
            mqttc.loop_stop()

        packet_in = fake_broker.receive_packet(1)
        assert not packet_in  # Check connection is closed
