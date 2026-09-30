# Tests for pluggable session-state persistence (paho.mqtt.persistence).
#
# A fake broker is driven over a socketpair that is injected into the
# client by overriding _create_socket. A "process restart" is simulated
# by discarding the Client and building a new one over the same
# FileSessionStore, client id and clean_session=False.

import os
import select
import socket
import threading
import time

import pytest

import paho.mqtt.client as mqtt
import tests.paho_test as paho_test
from paho.mqtt.enums import CallbackAPIVersion, MessageState
from paho.mqtt.persistence import (
    FileSessionStore,
    SessionSnapshot,
    SessionStoreCorrupted,
    StoredMessage,
)


class PipeEnd:
    """Minimal socket-like object built on os.pipe file descriptors.

    The test sandbox forbids real sockets, so the client<->broker
    transport and the client's internal wakeup pair are emulated with
    pipes, which support the same send/recv/select semantics.
    """

    def __init__(self, read_fd, write_fd):
        self._read_fd = read_fd
        self._write_fd = write_fd
        self._timeout = None
        self._closed = False

    def fileno(self):
        return self._read_fd

    def setblocking(self, flag):
        os.set_blocking(self._read_fd, flag)
        os.set_blocking(self._write_fd, flag)

    def settimeout(self, timeout):
        self._timeout = timeout

    def gettimeout(self):
        return self._timeout

    def send(self, data):
        return os.write(self._write_fd, bytes(data))

    def sendall(self, data):
        view = memoryview(bytes(data))
        while view:
            view = view[os.write(self._write_fd, view):]

    def recv(self, size):
        if self._timeout is not None:
            readable, _, _ = select.select([self._read_fd], [], [], self._timeout)
            if not readable:
                raise socket.timeout("timed out")
        return os.read(self._read_fd, size)

    def close(self):
        if not self._closed:
            self._closed = True
            os.close(self._read_fd)
            os.close(self._write_fd)


def pipe_pair():
    """Return two connected PipeEnd objects."""
    a_read, b_write = os.pipe()
    b_read, a_write = os.pipe()
    return PipeEnd(a_read, a_write), PipeEnd(b_read, b_write)


class PipeClient(mqtt.Client):
    """A Client whose TCP connection is replaced by a pipe pair, so a
    test can play the broker on the other end without any network."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.broker_end: PipeEnd | None = None
        self.crashed = False

    def _create_socket(self):
        if self.crashed:
            # The "process" is gone; its reconnect attempts must fail so
            # the network thread can be joined.
            raise OSError("simulated process crash")
        client_end, broker_end = pipe_pair()
        self.broker_end = broker_end
        return client_end


@pytest.fixture(autouse=True)
def _pipe_socketpair(monkeypatch):
    # The sandbox these tests run in forbids real sockets entirely;
    # paho's _socketpair_compat uses a loopback TCP pair, so swap it for
    # an os.pipe based pair (same behaviour for the client).
    monkeypatch.setattr(mqtt, "_socketpair_compat", pipe_pair)


def make_client(client_id, store_path, on_message=None, clean_session=False):
    client = PipeClient(
        CallbackAPIVersion.VERSION2,
        client_id=client_id,
        clean_session=clean_session,
        session_store=FileSessionStore(store_path),
    )
    if on_message is not None:
        client.on_message = on_message
    return client


def connect_packet(client_id, clean_session=False):
    return paho_test.gen_connect(
        client_id, keepalive=60, clean_session=clean_session)


def start(client):
    """Connect the client and return the broker end of the pipe."""
    client.connect("localhost", 1883)
    client.loop_start()
    assert client.broker_end is not None
    client.broker_end.settimeout(15)
    return client.broker_end


def crash(client):
    """Simulate a process crash: stop the network loop without DISCONNECT
    and drop every reference to the connection."""
    client.crashed = True
    if client.broker_end is not None:
        client.broker_end.close()
        client.broker_end = None
    client.loop_stop()


def wait_for(predicate, timeout=5.0, what="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


def wait_store(store_path, predicate, what):
    store = FileSessionStore(store_path)
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        try:
            snapshot = store.load()
        except SessionStoreCorrupted:
            time.sleep(0.02)
            continue
        if predicate(snapshot):
            return snapshot
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


# ---------------------------------------------------------------------------
# FileSessionStore unit behaviour
# ---------------------------------------------------------------------------

class TestFileSessionStore:
    def test_roundtrip(self, tmp_path):
        path = tmp_path / "session.bin"
        store = FileSessionStore(path)
        snapshot = SessionSnapshot(
            last_mid=42,
            messages=[
                StoredMessage("out", 7, b"a/b", b"payload", 2, False, True,
                              int(MessageState.MQTT_MS_WAIT_FOR_PUBCOMP)),
                StoredMessage("in", 9, b"c/d", b"\x00\xff", 2, False, False,
                              int(MessageState.MQTT_MS_WAIT_FOR_PUBREL)),
            ],
        )
        store.save(snapshot)
        assert FileSessionStore(path).load() == snapshot

    def test_missing_file_is_empty(self, tmp_path):
        snapshot = FileSessionStore(tmp_path / "nope.bin").load()
        assert snapshot.last_mid == 0
        assert snapshot.messages == []

    def test_corrupted_checksum_detected(self, tmp_path):
        path = tmp_path / "session.bin"
        store = FileSessionStore(path)
        store.save(SessionSnapshot(last_mid=3))
        raw = bytearray(path.read_bytes())
        raw[len(raw) // 2] ^= 0xFF
        path.write_bytes(bytes(raw))
        with pytest.raises(SessionStoreCorrupted):
            store.load()

    def test_truncated_file_detected(self, tmp_path):
        path = tmp_path / "session.bin"
        store = FileSessionStore(path)
        store.save(SessionSnapshot(last_mid=3))
        raw = path.read_bytes()
        path.write_bytes(raw[: len(raw) // 2])
        with pytest.raises(SessionStoreCorrupted):
            store.load()

    def test_garbage_file_detected(self, tmp_path):
        path = tmp_path / "session.bin"
        path.write_bytes(b"this is not a session store")
        with pytest.raises(SessionStoreCorrupted):
            FileSessionStore(path).load()

    def test_atomic_write_leaves_no_tempfile(self, tmp_path):
        path = tmp_path / "session.bin"
        FileSessionStore(path).save(SessionSnapshot(last_mid=1))
        assert [p.name for p in tmp_path.iterdir()] == ["session.bin"]

    def test_clear(self, tmp_path):
        path = tmp_path / "session.bin"
        store = FileSessionStore(path)
        store.save(SessionSnapshot(last_mid=1))
        store.clear()
        assert not path.exists()
        assert store.load() == SessionSnapshot()

    def test_client_refuses_corrupted_store(self, tmp_path):
        path = tmp_path / "session.bin"
        path.write_bytes(b"garbage")
        with pytest.raises(SessionStoreCorrupted):
            make_client("corrupt-client", path)


# ---------------------------------------------------------------------------
# Client restart scenarios against a fake broker
# ---------------------------------------------------------------------------

def test_outbound_qos1_restart_resends_dup_and_does_not_reuse_mid(tmp_path):
    store_path = tmp_path / "s.bin"
    client_id = "persist-qos1"

    client1 = make_client(client_id, store_path)
    conn = start(client1)
    paho_test.expect_packet(conn, "connect", connect_packet(client_id))
    conn.sendall(paho_test.gen_connack(rc=0))

    info1 = client1.publish("persist/qos1", "message", qos=1)
    assert info1.mid == 1
    paho_test.expect_packet(
        conn, "publish",
        paho_test.gen_publish("persist/qos1", qos=1, mid=1, payload="message"))
    # Crash before the PUBACK arrives.
    crash(client1)

    # "Restart": new client, same store and client id.
    client2 = make_client(client_id, store_path)
    # Packet id of the in-flight message must not be reused.
    assert client2._last_mid == 1
    conn = start(client2)
    paho_test.expect_packet(conn, "connect", connect_packet(client_id))
    conn.sendall(paho_test.gen_connack(flags=1, rc=0))

    paho_test.expect_packet(
        conn, "retried publish",
        paho_test.gen_publish("persist/qos1", qos=1, mid=1, payload="message", dup=True))
    conn.sendall(paho_test.gen_puback(1))

    # The next publish must use a fresh packet id.
    info2 = client2.publish("persist/qos1", "message2", qos=1)
    assert info2.mid == 2
    paho_test.expect_packet(
        conn, "second publish",
        paho_test.gen_publish("persist/qos1", qos=1, mid=2, payload="message2"))
    conn.sendall(paho_test.gen_puback(2))

    wait_store(store_path, lambda s: not s.messages, "store to be drained")
    client2.loop_stop()


def test_outbound_qos2_restart_after_pubrec_resends_pubrel(tmp_path):
    store_path = tmp_path / "s.bin"
    client_id = "persist-qos2"

    client1 = make_client(client_id, store_path)
    conn = start(client1)
    paho_test.expect_packet(conn, "connect", connect_packet(client_id))
    conn.sendall(paho_test.gen_connack(rc=0))

    client1.publish("persist/qos2", "message", qos=2)
    paho_test.expect_packet(
        conn, "publish",
        paho_test.gen_publish("persist/qos2", qos=2, mid=1, payload="message"))
    conn.sendall(paho_test.gen_pubrec(1))
    paho_test.expect_packet(conn, "pubrel", paho_test.gen_pubrel(1))
    # Wait until the PUBCOMP-waiting state is durable, then crash.
    wait_store(
        store_path,
        lambda s: any(m.direction == "out" and m.mid == 1
                      and m.state == int(MessageState.MQTT_MS_WAIT_FOR_PUBCOMP)
                      for m in s.messages),
        "outbound QoS 2 PUBREL stage to be persisted")
    crash(client1)

    client2 = make_client(client_id, store_path)
    conn = start(client2)
    paho_test.expect_packet(conn, "connect", connect_packet(client_id))
    # Broker still has the session: only the PUBREL must be repeated.
    conn.sendall(paho_test.gen_connack(flags=1, rc=0))
    paho_test.expect_packet(conn, "retried pubrel", paho_test.gen_pubrel(1))
    conn.sendall(paho_test.gen_pubcomp(1))

    wait_store(store_path, lambda s: not s.messages, "store to be drained")
    client2.loop_stop()


def test_outbound_qos2_restart_without_broker_session_republishes(tmp_path):
    store_path = tmp_path / "s.bin"
    client_id = "persist-qos2-nosession"

    client1 = make_client(client_id, store_path)
    conn = start(client1)
    paho_test.expect_packet(conn, "connect", connect_packet(client_id))
    conn.sendall(paho_test.gen_connack(rc=0))

    client1.publish("persist/qos2", "message", qos=2)
    paho_test.expect_packet(
        conn, "publish",
        paho_test.gen_publish("persist/qos2", qos=2, mid=1, payload="message"))
    conn.sendall(paho_test.gen_pubrec(1))
    paho_test.expect_packet(conn, "pubrel", paho_test.gen_pubrel(1))
    wait_store(
        store_path,
        lambda s: any(m.state == int(MessageState.MQTT_MS_WAIT_FOR_PUBCOMP)
                      for m in s.messages),
        "outbound QoS 2 PUBREL stage to be persisted")
    crash(client1)

    client2 = make_client(client_id, store_path)
    conn = start(client2)
    paho_test.expect_packet(conn, "connect", connect_packet(client_id))
    # Broker lost the session (session_present=0): the exchange must
    # restart as a DUP PUBLISH, not a PUBREL for a message the broker
    # may never have seen.
    conn.sendall(paho_test.gen_connack(flags=0, rc=0))
    paho_test.expect_packet(
        conn, "republished message",
        paho_test.gen_publish("persist/qos2", qos=2, mid=1, payload="message", dup=True))
    conn.sendall(paho_test.gen_pubrec(1))
    paho_test.expect_packet(conn, "pubrel", paho_test.gen_pubrel(1))
    conn.sendall(paho_test.gen_pubcomp(1))

    wait_store(store_path, lambda s: not s.messages, "store to be drained")
    client2.loop_stop()


def test_inbound_qos2_restart_completes_on_pubrel(tmp_path):
    store_path = tmp_path / "s.bin"
    client_id = "persist-in-qos2"
    received = []
    got_message = threading.Event()

    def on_message(client, userdata, message):
        received.append(message.payload)
        got_message.set()

    client1 = make_client(client_id, store_path, on_message=on_message)
    conn = start(client1)
    paho_test.expect_packet(conn, "connect", connect_packet(client_id))
    conn.sendall(paho_test.gen_connack(rc=0))

    conn.sendall(paho_test.gen_publish(
        "persist/in", qos=2, mid=9, payload="inbound"))
    paho_test.expect_packet(conn, "pubrec", paho_test.gen_pubrec(9))
    # The inbound QoS 2 stage must be durable before we crash.
    wait_store(
        store_path,
        lambda s: any(m.direction == "in" and m.mid == 9
                      and m.state == int(MessageState.MQTT_MS_WAIT_FOR_PUBREL)
                      for m in s.messages),
        "inbound QoS 2 stage to be persisted")
    # Crash before the PUBREL arrives: the message was never delivered.
    crash(client1)
    assert received == []

    client2 = make_client(client_id, store_path, on_message=on_message)
    conn = start(client2)
    paho_test.expect_packet(conn, "connect", connect_packet(client_id))
    conn.sendall(paho_test.gen_connack(flags=1, rc=0))

    # Broker resends PUBREL; the restored client must deliver the
    # persisted message to the application and PUBCOMP.
    conn.sendall(paho_test.gen_pubrel(9))
    paho_test.expect_packet(conn, "pubcomp", paho_test.gen_pubcomp(9))
    assert got_message.wait(timeout=5)
    assert received == [b"inbound"]

    wait_store(store_path, lambda s: not s.messages, "store to be drained")
    client2.loop_stop()


def test_inbound_qos2_restart_without_broker_session_drops_stage(tmp_path):
    store_path = tmp_path / "s.bin"
    client_id = "persist-in-qos2-nosession"
    received = []

    def on_message(client, userdata, message):
        received.append(message.payload)

    client1 = make_client(client_id, store_path, on_message=on_message)
    conn = start(client1)
    paho_test.expect_packet(conn, "connect", connect_packet(client_id))
    conn.sendall(paho_test.gen_connack(rc=0))

    conn.sendall(paho_test.gen_publish("persist/in", qos=2, mid=9, payload="inbound"))
    paho_test.expect_packet(conn, "pubrec", paho_test.gen_pubrec(9))
    wait_store(
        store_path,
        lambda s: any(m.direction == "in" for m in s.messages),
        "inbound QoS 2 stage to be persisted")
    crash(client1)

    client2 = make_client(client_id, store_path, on_message=on_message)
    conn = start(client2)
    paho_test.expect_packet(conn, "connect", connect_packet(client_id))
    # Broker lost its session: the stale inbound QoS 2 stage is dropped.
    conn.sendall(paho_test.gen_connack(flags=0, rc=0))
    wait_store(
        store_path,
        lambda s: not any(m.direction == "in" for m in s.messages),
        "stale inbound QoS 2 stage to be dropped")

    # The broker starts the delivery from scratch with a new exchange.
    conn.sendall(paho_test.gen_publish("persist/in", qos=2, mid=4, payload="inbound"))
    paho_test.expect_packet(conn, "pubrec", paho_test.gen_pubrec(4))
    conn.sendall(paho_test.gen_pubrel(4))
    paho_test.expect_packet(conn, "pubcomp", paho_test.gen_pubcomp(4))
    wait_for(lambda: received == [b"inbound"], what="message delivery")

    client2.loop_stop()


def test_clean_session_connect_drops_persisted_inbound_state(tmp_path):
    store_path = tmp_path / "s.bin"
    client_id = "persist-clean"

    client1 = make_client(client_id, store_path)
    conn = start(client1)
    paho_test.expect_packet(conn, "connect", connect_packet(client_id))
    conn.sendall(paho_test.gen_connack(rc=0))

    conn.sendall(paho_test.gen_publish("persist/in", qos=2, mid=9, payload="inbound"))
    paho_test.expect_packet(conn, "pubrec", paho_test.gen_pubrec(9))
    wait_store(
        store_path,
        lambda s: any(m.direction == "in" for m in s.messages),
        "inbound QoS 2 stage to be persisted")
    crash(client1)

    # Restart with clean_session=True: the persisted inbound exchange
    # must be cleaned up rather than resumed.
    client2 = make_client(client_id, store_path, clean_session=True)
    conn = start(client2)
    paho_test.expect_packet(
        conn, "connect", connect_packet(client_id, clean_session=True))
    conn.sendall(paho_test.gen_connack(flags=0, rc=0))

    wait_store(
        store_path,
        lambda s: not any(m.direction == "in" for m in s.messages),
        "inbound state to be cleaned on clean session")
    client2.loop_stop()
