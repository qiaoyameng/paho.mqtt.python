"""Tests for persistent MQTT session state.

The fake broker uses an in-memory pipe (see tests/testsupport/fake_pipe.py)
because the sandbox running these tests forbids real sockets. The tests
simulate a process restart by destroying the Client object while keeping the
same FileSessionStore directory and client id, then creating a brand new
Client that reconnects.

Important semantic note: persistence resumes the QoS 1/2 protocol exchanges,
it does not guarantee exactly-once application side effects. A test below
explicitly documents that the on_message callback for a QoS 2 message can run
again when the crash window covers PUBREL handling.
"""
from __future__ import annotations

import os
import threading
import time

import pytest

import paho.mqtt.client as paho_client
from paho.mqtt.enums import CallbackAPIVersion, MQTTProtocolVersion
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.sessionstore import (
    CorruptSessionError,
    FileSessionStore,
    PersistentMessage,
    SessionState,
    state_from_dict,
    state_to_dict,
)

import tests.paho_test as paho_test
from tests.testsupport.fake_pipe import (
    PipeBroker,
    install_socketpair_patch,
    read_packet,
)

CLIENT_ID = "persistent-session-client"
KEEPALIVE = 60


@pytest.fixture(autouse=True)
def patched_socketpair():
    original_select = paho_client.select.select
    install_socketpair_patch()
    yield
    paho_client.select.select = original_select


def _connect_packet():
    return paho_test.gen_connect(CLIENT_ID, keepalive=KEEPALIVE,
                                 clean_session=False)


def _make_client(store, broker, **kwargs):
    client = paho_client.Client(
        CallbackAPIVersion.VERSION2,
        CLIENT_ID,
        clean_session=False,
        protocol=kwargs.pop("protocol", MQTTProtocolVersion.MQTTv311),
        session_store=store,
        **kwargs,
    )
    broker.attach_client(client)
    return client


def _wait_event(event, timeout=5.0):
    assert event.wait(timeout), "condition was not met in time"


def _start(client, broker):
    connected = threading.Event()
    client.on_connect = lambda *args: connected.set()
    client.connect_async("localhost", 1, keepalive=KEEPALIVE)
    client.loop_start()
    broker_in, broker_out = broker.next_connection()
    assert read_packet(broker_in) == _connect_packet()
    broker.send(broker_out, paho_test.gen_connack(rc=0, flags=1))
    _wait_event(connected)
    return broker_in, broker_out


# ---------------------------------------------------------------------------
# FileSessionStore reference implementation
# ---------------------------------------------------------------------------


class TestFileSessionStore:
    def test_round_trip(self, tmp_path):
        store = FileSessionStore(str(tmp_path))
        message = PersistentMessage(
            mid=42, topic=b"t", payload=b"p", qos=2, state=3
        )
        state = SessionState(out_messages={42: message}, last_mid=42)

        assert store.load("client-a") is None
        store.save("client-a", state)
        loaded = store.load("client-a")
        assert loaded is not None
        assert loaded.last_mid == 42
        assert loaded.out_messages[42].topic == b"t"
        assert loaded.out_messages[42].payload == b"p"
        assert loaded.out_messages[42].qos == 2

    def test_client_ids_have_independent_files(self, tmp_path):
        store = FileSessionStore(str(tmp_path))
        store.save("a", SessionState(last_mid=1))
        store.save("b", SessionState(last_mid=2))
        assert store.load("a").last_mid == 1
        assert store.load("b").last_mid == 2

    def test_delete(self, tmp_path):
        store = FileSessionStore(str(tmp_path))
        store.save("a", SessionState(last_mid=1))
        store.delete("a")
        store.delete("a")  # deleting a missing session must not raise
        assert store.load("a") is None

    @pytest.mark.parametrize("mutation", [
        # truncate the tail
        lambda blob: blob[:-3],
        # corrupt the checksum area
        lambda blob: blob[:20] + b"\xff" + blob[21:],
        # corrupt the payload but keep the length
        lambda blob: blob[:-1] + (b"x" if blob[-1:] != b"x" else b"y"),
        # replace the magic
        lambda blob: b"X" + blob[1:],
    ])
    def test_corruption_is_detected(self, tmp_path, mutation):
        store = FileSessionStore(str(tmp_path))
        store.save("a", SessionState(last_mid=1))
        path = store._path("a")
        with open(path, "rb") as handle:
            blob = handle.read()
        with open(path, "wb") as handle:
            handle.write(mutation(blob))
        with pytest.raises(CorruptSessionError):
            store.load("a")

    def test_unknown_version_is_rejected(self, tmp_path):
        store = FileSessionStore(str(tmp_path))
        store.save("a", SessionState(last_mid=1))
        path = store._path("a")
        with open(path, "rb") as handle:
            blob = bytearray(handle.read())
        blob[8] = 99
        with open(path, "wb") as handle:
            handle.write(bytes(blob))
        with pytest.raises(CorruptSessionError):
            store.load("a")

    def test_properties_round_trip(self):
        properties = Properties(PacketTypes.PUBLISH)
        properties.ContentType = "text/plain"
        message = PersistentMessage(
            mid=7, topic=b"tp", payload=b"", qos=1, state=2,
            properties=properties,
        )
        restored = state_from_dict(state_to_dict(
            SessionState(out_messages={7: message}, last_mid=7)
        ))
        assert restored.out_messages[7].properties.ContentType == "text/plain"

    def test_malformed_payload_raises(self):
        with pytest.raises(CorruptSessionError):
            state_from_dict(["not", "a", "dict"])
        with pytest.raises(CorruptSessionError):
            state_from_dict({"last_mid": 70000,
                             "out_messages": [], "in_messages": []})


# ---------------------------------------------------------------------------
# Fake broker disconnect/restart scenarios (synchronously driven)
# ---------------------------------------------------------------------------


class _BrokerSession:
    """One TCP connection of a synchronously driven fake broker."""

    def __init__(self, broker, connack_flags=1):
        self.broker = broker
        self.client = None
        self.broker_in = None
        self.broker_out = None
        self.connack_flags = connack_flags

    def open_with(self, client):
        self.client = client
        client.connect_async("localhost", 1, keepalive=KEEPALIVE)
        client.reconnect()
        self.broker_in, self.broker_out = self.broker.next_connection()
        client.loop(0.01)
        connect_packet = read_packet(self.broker_in)
        self.broker.send(self.broker_out,
                         paho_test.gen_connack(rc=0, flags=self.connack_flags))
        client.loop(0.01)
        return connect_packet

    def send(self, packet):
        self.broker.send(self.broker_out, packet)
        self.client.loop(0.01)
        self.client.loop(0.01)

    def recv(self):
        self.client.loop(0.01)
        self.client.loop(0.01)
        return read_packet(self.broker_in)

    def drop(self):
        self.broker.drop((self.broker_in, self.broker_out))
        self.client.loop(0.01)  # notice the loss


def _client(store, broker):
    client = _make_client(store, broker)
    client._sockpairR, client._sockpairW = (
        paho_client._socketpair_compat()
    )
    return client


def _restart(store, broker, connack_flags=1):
    """Simulate a new client process reusing the same persisted store."""
    client = _client(store, broker)
    session = _BrokerSession(broker, connack_flags=connack_flags)
    connect_packet = session.open_with(client)
    return client, session, connect_packet


class TestOutboundQoS1:
    def test_puback_lost_then_restart_resends_with_dup(self, tmp_path):
        store = FileSessionStore(str(tmp_path))
        broker = PipeBroker()
        try:
            client, session, connect_packet = _restart(store, broker)
            assert connect_packet == _connect_packet()

            info = client.publish("qos1/topic", "hello", qos=1)
            mid = info.mid
            assert session.recv() == paho_test.gen_publish(
                "qos1/topic", qos=1, mid=mid, payload="hello"
            )
            session.drop()
            del client

            client, session, _ = _restart(store, broker)
            assert session.recv() == paho_test.gen_publish(
                "qos1/topic", qos=1, mid=mid, payload="hello", dup=True
            )
            session.send(paho_test.gen_puback(mid))
            assert client._out_messages == {}
        finally:
            broker.close()


class TestOutboundQoS2:
    def test_restart_after_publish_and_after_pubrel(self, tmp_path):
        store = FileSessionStore(str(tmp_path))
        broker = PipeBroker()
        try:
            client, session, _ = _restart(store, broker)
            info = client.publish("qos2/topic", "world", qos=2)
            mid = info.mid
            assert session.recv() == paho_test.gen_publish(
                "qos2/topic", qos=2, mid=mid, payload="world"
            )

            # Crash after PUBLISH was sent, before PUBREC.
            session.drop()
            del client
            client, session, _ = _restart(store, broker)
            assert session.recv() == paho_test.gen_publish(
                "qos2/topic", qos=2, mid=mid, payload="world", dup=True
            )
            session.send(paho_test.gen_pubrec(mid))
            assert session.recv() == paho_test.gen_pubrel(mid)

            # Crash after PUBREL was sent, before PUBCOMP.
            session.drop()
            del client
            client, session, _ = _restart(store, broker)
            assert session.recv() == paho_test.gen_pubrel(mid)
            session.send(paho_test.gen_pubcomp(mid))
            assert client._out_messages == {}
        finally:
            broker.close()


class TestInboundQoS2:
    def test_restart_between_pubrec_and_pubrel(self, tmp_path):
        store = FileSessionStore(str(tmp_path))
        broker = PipeBroker()
        try:
            client, session, _ = _restart(store, broker)
            mid = 100
            session.send(paho_test.gen_publish(
                "in/qos2", qos=2, mid=mid, payload="incoming"
            ))
            assert session.recv() == paho_test.gen_pubrec(mid)
            assert mid in client._in_messages

            session.drop()
            del client
            client, session, _ = _restart(store, broker)

            # A retransmitted PUBLISH must be answered with a retransmitted
            # PUBREC and must not trigger on_message again.
            session.send(paho_test.gen_publish(
                "in/qos2", qos=2, mid=mid, payload="incoming", dup=True
            ))
            assert session.recv() == paho_test.gen_pubrec(mid)

            session.send(paho_test.gen_pubrel(mid))
            assert session.recv() == paho_test.gen_pubcomp(mid)
            assert mid not in client._in_messages
        finally:
            broker.close()

    def test_application_side_effect_can_be_duplicated(self, tmp_path):
        # Persistence does not provide exactly-once application delivery:
        # crash after PUBREL (callback ran) but before PUBCOMP reaches the
        # broker leads the broker to retry PUBREL and the callback runs again
        # after restart. Handlers must be idempotent.
        store = FileSessionStore(str(tmp_path))
        broker = PipeBroker()
        try:
            client, session, _ = _restart(store, broker)
            first = threading.Event()
            client.on_message = lambda *a: first.set()
            mid = 200
            session.send(paho_test.gen_publish(
                "in/qos2", qos=2, mid=mid, payload="at-least-once"
            ))
            assert session.recv() == paho_test.gen_pubrec(mid)
            session.send(paho_test.gen_pubrel(mid))
            assert session.recv() == paho_test.gen_pubcomp(mid)
            assert first.wait(2)

            # Emulate a crash in the tiny window where PUBREL has been
            # handled (callback executed) but the PUBCOMP write did not
            # reach the broker: on restart the store still contains the
            # incoming exchange in its wait-for-PUBREL state.
            session.drop()
            restored = store.load(CLIENT_ID)
            restored.in_messages[mid] = PersistentMessage(
                mid=mid, topic=b"in/qos2", payload=b"at-least-once", qos=2,
                state=5,
            )
            store.save(CLIENT_ID, restored)
            del client

            second = threading.Event()
            client, session, _ = _restart(store, broker)
            client.on_message = lambda *a: second.set()
            session.send(paho_test.gen_pubrel(mid, dup=True))
            assert session.recv() == paho_test.gen_pubcomp(mid)
            assert second.wait(2)
        finally:
            broker.close()


class TestPacketIdAllocation:
    def test_mid_generator_skips_inflight_and_incoming_ids(self):
        client = paho_client.Client(
            CallbackAPIVersion.VERSION2,
            "mid-generator",
            clean_session=False,
        )
        assert client._mid_generate() == 1
        assert client._mid_generate() == 2
        # Pretend id 3 is in flight.
        from paho.mqtt.client import MQTTMessage
        client._out_messages[3] = MQTTMessage(3, b"t")
        assert client._mid_generate() == 4
        # An incoming QoS 2 exchange also reserves its id.
        client._in_messages[5] = MQTTMessage(5, b"t")
        assert client._mid_generate() == 6

    def test_mid_generator_wraps_past_reserved_ids(self):
        client = paho_client.Client(
            CallbackAPIVersion.VERSION2,
            "mid-wrap",
            clean_session=False,
        )
        from paho.mqtt.client import MQTTMessage
        client._last_mid = 65534
        # Ids 65535 and 1 are in use; wrapping must land on 2.
        client._out_messages[65535] = MQTTMessage(65535, b"t")
        client._out_messages[1] = MQTTMessage(1, b"t")
        assert client._mid_generate() == 2

    def test_mid_is_not_reused_for_pending_handshake(self, tmp_path):
        store = FileSessionStore(str(tmp_path))
        broker = PipeBroker()
        try:
            client, session, _ = _restart(store, broker)
            info = client.publish("qos1/id", "x", qos=1)
            mid = info.mid
            assert session.recv() == paho_test.gen_publish(
                "qos1/id", qos=1, mid=mid, payload="x"
            )
            session.drop()
            del client

            client, session, _ = _restart(store, broker)
            assert session.recv() == paho_test.gen_publish(
                "qos1/id", qos=1, mid=mid, payload="x", dup=True
            )
            new_info = client.publish("qos1/other", "y", qos=1)
            assert new_info.mid != mid
            assert mid in client._out_messages
            session.send(paho_test.gen_puback(mid))
            session.send(paho_test.gen_puback(new_info.mid))
        finally:
            broker.close()


class TestSessionPresentHandling:
    def test_session_absent_clears_incoming_half(self, tmp_path):
        store = FileSessionStore(str(tmp_path))
        broker = PipeBroker()
        try:
            client, session, _ = _restart(store, broker, connack_flags=1)
            mid = 300
            session.send(paho_test.gen_publish(
                "in/qos2", qos=2, mid=mid, payload="orphaned"
            ))
            assert session.recv() == paho_test.gen_pubrec(mid)
            session.drop()
            del client

            # Broker reports no existing session: SP=0 clears the incoming
            # half which the new broker session cannot complete anyway.
            client, session, _ = _restart(store, broker, connack_flags=0)
            assert client._in_messages == {}
        finally:
            broker.close()


class TestCleanSession:
    def test_clean_start_discards_persisted_state(self, tmp_path):
        store = FileSessionStore(str(tmp_path))
        store.save(CLIENT_ID, SessionState(last_mid=55))
        assert store.load(CLIENT_ID) is not None

        broker = PipeBroker()
        try:
            client = _client(store, broker)
            client._clean_session = True
            client.connect_async("localhost", 1, keepalive=KEEPALIVE)
            client.reconnect()
            assert store.load(CLIENT_ID) is None
        finally:
            broker.close()

    def test_corrupt_store_starts_fresh(self, tmp_path):
        store = FileSessionStore(str(tmp_path))
        store.save(CLIENT_ID, SessionState(last_mid=1))
        with open(store._path(CLIENT_ID), "wb") as handle:
            handle.write(b"this is definitely not a session file")

        broker = PipeBroker()
        try:
            client = _client(store, broker)
            session = _BrokerSession(broker, connack_flags=0)
            session.open_with(client)
            # The corrupt file was dropped; a fresh (empty) state was saved
            # once the connection succeeded.
            restored = store.load(CLIENT_ID)
            assert restored is not None
            assert restored.out_messages == {}
            assert restored.in_messages == {}
        finally:
            broker.close()


class TestMQTTv5:
    def _v5_client(self, store, broker, clean_start=None):
        if clean_start is None:
            clean_start = paho_client.MQTT_CLEAN_START_FIRST_ONLY
        client = paho_client.Client(
            CallbackAPIVersion.VERSION2,
            CLIENT_ID,
            protocol=MQTTProtocolVersion.MQTTv5,
            session_store=store,
        )
        client._sockpairR, client._sockpairW = (
            paho_client._socketpair_compat()
        )
        broker.attach_client(client)
        client.connect_async("localhost", 1, keepalive=KEEPALIVE,
                             clean_start=clean_start)
        return client

    def test_first_connect_cleans_store_later_connects_restore(self, tmp_path):
        store = FileSessionStore(str(tmp_path))
        broker = PipeBroker()
        try:
            # Leftover state from a previous process is deleted on the first
            # clean_start=FIRST_ONLY connection.
            store.save(CLIENT_ID, SessionState(last_mid=9))

            client = self._v5_client(store, broker)
            client.reconnect()
            broker_in, broker_out = broker.next_connection()
            client.loop(0.01)
            assert read_packet(broker_in) == paho_test.gen_connect(
                CLIENT_ID, keepalive=KEEPALIVE, proto_ver=5,
                clean_session=True, properties=None
            )
            broker.send(broker_out, paho_test.gen_connack(rc=0, flags=1,
                                                          proto_ver=5))
            client.loop(0.01)
            # Leftover state was deleted before CONNECT; the successful
            # connection writes a fresh (empty) state.
            assert store.load(CLIENT_ID) == SessionState(
                out_messages={}, in_messages={}, last_mid=0
            )

            # A later connection (same process model) must NOT be clean.
            client.reconnect()
            broker_in, broker_out = broker.next_connection()
            client.loop(0.01)
            assert read_packet(broker_in) == paho_test.gen_connect(
                CLIENT_ID, keepalive=KEEPALIVE, proto_ver=5,
                clean_session=False, properties=None
            )
            broker.send(broker_out, paho_test.gen_connack(rc=0, flags=1,
                                                          proto_ver=5))
            client.loop(0.01)
        finally:
            broker.close()

    def test_qos2_with_properties_survives_restart(self, tmp_path):
        store = FileSessionStore(str(tmp_path))
        broker = PipeBroker()
        try:
            client = self._v5_client(store, broker)
            client.reconnect()
            broker_in, broker_out = broker.next_connection()
            client.loop(0.01)
            read_packet(broker_in)
            broker.send(broker_out, paho_test.gen_connack(rc=0, flags=1,
                                                          proto_ver=5))
            client.loop(0.01)

            properties = Properties(PacketTypes.PUBLISH)
            properties.ContentType = "application/json"
            packed_props = properties.pack()
            # Properties.pack() includes the properties-length VBI prefix;
            # gen_publish adds that prefix itself, so decode and strip it.
            _vbi_len = 1
            while packed_props[_vbi_len - 1] & 0x80:
                _vbi_len += 1
            raw_props = packed_props[_vbi_len:]
            info = client.publish(
                "v5/qos2", payload=b'{"k":1}', qos=2, properties=properties
            )
            mid = info.mid
            first = read_packet(broker_in)
            assert first == paho_test.gen_publish(
                "v5/qos2", qos=2, mid=mid, payload='{"k":1}', proto_ver=5,
                properties=raw_props,
            )
        finally:
            broker.close()

        # New process resumes the exchange with the same properties.
        broker = PipeBroker()
        try:
            client = self._v5_client(store, broker)
            client._mqttv5_first_connect = False  # resumed process, not first
            client.reconnect()
            broker_in, broker_out = broker.next_connection()
            client.loop(0.01)
            assert read_packet(broker_in) == paho_test.gen_connect(
                CLIENT_ID, keepalive=KEEPALIVE, proto_ver=5,
                clean_session=False, properties=None
            )
            broker.send(broker_out, paho_test.gen_connack(rc=0, flags=1,
                                                          proto_ver=5))
            client.loop(0.01)
            retried = read_packet(broker_in)
            assert retried == paho_test.gen_publish(
                "v5/qos2", qos=2, mid=mid, payload='{"k":1}', proto_ver=5,
                dup=True, properties=raw_props,
            )
        finally:
            broker.close()
