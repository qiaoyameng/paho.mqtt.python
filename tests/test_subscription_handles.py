"""Tests for the MQTT v5 Subscription Identifier subscription handle API.

No real broker or socket is used (the sandbox forbids socket creation): the
client is driven in-memory by injecting raw packets straight into its packet
handler, while the outgoing SUBSCRIBE/UNSUBSCRIBE packets are captured. This
also lets us reproduce "misbehaving" peers: a PUBLISH before SUBACK, partial
subscription rejection and a stale SUBACK arriving after a reconnect.
"""

import struct
import time
import unittest
import queue

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.enums import _ConnectionState
from paho.mqtt.properties import VariableByteIntegers
from paho.mqtt.subscription import SubscriptionState


def encode_vbi(value):
    return bytes(VariableByteIntegers.encode(value))


def decode_vbi(buf, offset=0):
    multiplier = 1
    value = 0
    while True:
        digit = buf[offset]
        offset += 1
        value += (digit & 127) * multiplier
        if digit & 128 == 0:
            break
        multiplier *= 128
    return value, offset


def pack_str(text):
    if isinstance(text, str):
        text = text.encode("utf-8")
    return struct.pack("!H", len(text)) + text


def encode_packet(command, payload):
    remaining = len(payload)
    out = bytearray([command])
    while True:
        digit = remaining % 128
        remaining //= 128
        if remaining > 0:
            digit |= 0x80
        out.append(digit)
        if remaining == 0:
            break
    out += payload
    return bytes(out)


def parse_subscribe_packet(packet_bytes):
    """Parse a full SUBSCRIBE packet. Returns (mid, [topics], identifier)."""
    # skip fixed header (command + remaining length VBI)
    offset = 1
    while packet_bytes[offset] & 0x80:
        offset += 1
    offset += 1
    payload = packet_bytes[offset:]
    mid = struct.unpack("!H", payload[:2])[0]
    props_len, pos = decode_vbi(payload, 2)
    props = payload[pos:pos + props_len]
    pos += props_len
    identifier = None
    while props:
        prop_id, n = decode_vbi(props, 0)
        props = props[n:]
        if prop_id == 11:
            identifier, n = decode_vbi(props, 0)
            props = props[n:]
    topics = []
    while pos < len(payload):
        tlen = struct.unpack("!H", payload[pos:pos + 2])[0]
        pos += 2
        topics.append(payload[pos:pos + tlen].decode("utf-8"))
        pos += tlen
        pos += 1  # options byte
    return mid, topics, identifier


def parse_unsubscribe_packet(packet_bytes):
    offset = 1
    while packet_bytes[offset] & 0x80:
        offset += 1
    offset += 1
    payload = packet_bytes[offset:]
    mid = struct.unpack("!H", payload[:2])[0]
    props_len, pos = decode_vbi(payload, 2)
    pos += props_len
    topics = []
    while pos < len(payload):
        tlen = struct.unpack("!H", payload[pos:pos + 2])[0]
        pos += 2
        topics.append(payload[pos:pos + tlen].decode("utf-8"))
        pos += tlen
    return mid, topics


def make_suback(mid, reason_codes):
    payload = struct.pack("!H", mid) + encode_vbi(0) + bytes(reason_codes)
    return encode_packet(0x90, payload)


def make_unsuback(mid, reason_codes=(0,)):
    payload = struct.pack("!H", mid) + encode_vbi(0) + bytes(reason_codes)
    return encode_packet(0xB0, payload)


def make_publish(topic, payload, identifiers=(), qos=0):
    props = b""
    for identifier in identifiers:
        props += encode_vbi(11) + encode_vbi(identifier)
    body = pack_str(topic) + encode_vbi(len(props)) + props + payload
    command = 0x30 | (qos << 1)
    if qos:
        body += struct.pack("!H", 1)
    return encode_packet(command, body)


def make_connack():
    return encode_packet(0x20, b"\x00\x00\x00")


class Harness:
    """In-memory MQTT v5 client: inject packets, capture outgoing ones."""

    def __init__(self):
        self.client = mqtt.Client(
            CallbackAPIVersion.VERSION2,
            client_id="harness",
            protocol=mqtt.MQTTv5,
        )
        self.events = queue.Queue()
        self.sent_subscribes = []   # list of parsed (mid, topics, id)
        self.sent_unsubscribes = []  # list of parsed (mid, topics)

        client = self.client

        def on_connect(c, userdata, flags, reason_code, properties):
            self.events.put(("connect", reason_code.value))

        def on_subscribe(c, userdata, mid, reason_codes, properties):
            self.events.put(("suback", mid, [r.value for r in reason_codes]))

        def on_unsubscribe(c, userdata, mid, reason_codes, properties):
            self.events.put(("unsuback", mid, [r.value for r in reason_codes]))

        client.on_connect = on_connect
        client.on_subscribe = on_subscribe
        client.on_unsubscribe = on_unsubscribe

        # Avoid any attempt at real network I/O: outgoing packets are only
        # queued, never written by the client itself.
        client.on_socket_register_write = lambda c, u, s: None
        client.on_socket_unregister_write = lambda c, u, s: None

        real_send_subscribe = client._send_subscribe
        real_send_unsubscribe = client._send_unsubscribe

        def fake_send_subscribe(dup, topics, properties=None):
            result, mid = real_send_subscribe(dup, topics, properties)
            # Pull the generated packet back out to parse its properties.
            packet = client._out_packet[-1]
            parsed = parse_subscribe_packet(bytes(packet["packet"]))
            self.sent_subscribes.append(parsed)
            return result, mid

        def fake_send_unsubscribe(dup, topics_list, properties=None):
            result, mid = real_send_unsubscribe(
                dup, topics_list, properties)
            packet = client._out_packet[-1]
            self.sent_unsubscribes.append(
                parse_unsubscribe_packet(bytes(packet["packet"])))
            return result, mid

        client._send_subscribe = fake_send_subscribe
        client._send_unsubscribe = fake_send_unsubscribe

    def connect(self):
        # Pretend the TCP connection exists and CONNACK was accepted.
        client = self.client

        class FakeSock:
            def close(self):
                pass

            def setblocking(self, flag):
                pass

        client._sock = FakeSock()
        client._state = _ConnectionState.MQTT_CS_CONNECTING
        self.inject(make_connack())

    def inject(self, raw_packet):
        """Feed a raw packet as if it came from the broker."""
        client = self.client
        # Decode fixed header the same way _packet_read does.
        offset = 1
        multiplier = 1
        remaining = 0
        while True:
            digit = raw_packet[offset]
            offset += 1
            remaining += (digit & 127) * multiplier
            if digit & 128 == 0:
                break
            multiplier *= 128
        client._in_packet = {
            "command": raw_packet[0],
            "have_remaining": 0,
            "remaining_count": [],
            "remaining_mult": 1,
            "remaining_length": remaining,
            "packet": bytearray(raw_packet[offset:]),
            "to_process": 0,
            "pos": 0,
        }
        rc = client._packet_handle()
        return rc

    def sub_mid_for(self, identifier):
        for mid, topics, ident in self.sent_subscribes:
            if ident == identifier:
                return mid
        raise KeyError(identifier)

    def wait_event(self, *expected, timeout=5):
        deadline = time.time() + timeout
        seen = []
        while time.time() < deadline:
            try:
                item = self.events.get(timeout=min(0.5, deadline - time.time()))
            except queue.Empty:
                continue
            if item[:len(expected)] == expected:
                return item
            seen.append(item)
        raise AssertionError(f"timed out waiting for {expected}; seen={seen}")


class SubscriptionHandleTests(unittest.TestCase):
    def setUp(self):
        self.h = Harness()
        self.h.connect()

    def tearDown(self):
        self.h.client._reset_sockets()

    def test_overlapping_wildcards_and_dedupe(self):
        """Overlapping wildcard subscriptions route to the right modules, and
        a PUBLISH carrying several identifiers (one duplicated) invokes each
        handler at most once, in broker order."""
        got_a, got_b = queue.Queue(), queue.Queue()

        def make_handler(box, label):
            def _h(c, userdata, message):
                box.put((label, message.topic, message.payload))
            return _h

        handle_a = self.h.client.subscribe_handle(
            "sport/tennis/+", make_handler(got_a, "A"), identifier=10)
        mid_a = self.h.sub_mid_for(10)
        handle_b = self.h.client.subscribe_handle(
            "sport/#", make_handler(got_b, "B"), identifier=20)
        mid_b = self.h.sub_mid_for(20)

        self.assertIs(handle_a.state, SubscriptionState.PENDING)
        self.assertIs(handle_b.state, SubscriptionState.PENDING)

        # SUBACK success activates the handlers.
        self.h.inject(make_suback(mid_a, [0]))
        self.h.inject(make_suback(mid_b, [1]))  # granted QoS 1
        self.assertIs(handle_a.state, SubscriptionState.ACTIVE)
        self.assertIs(handle_b.state, SubscriptionState.ACTIVE)
        self.assertEqual(handle_b.qos, 1)

        # Both match; identifier 10 deliberately repeated.
        self.h.inject(make_publish(
            "sport/tennis/player1", b"hello", identifiers=(10, 20, 10)))

        self.assertEqual(
            got_a.get(timeout=5), ("A", "sport/tennis/player1", b"hello"))
        self.assertEqual(
            got_b.get(timeout=5), ("B", "sport/tennis/player1", b"hello"))
        self.assertTrue(got_a.empty())
        self.assertTrue(got_b.empty())

        # The outgoing SUBSCRIBE packets really carried the identifiers.
        self.assertEqual(
            {ident for _m, _t, ident in self.h.sent_subscribes}, {10, 20})

    def test_same_callback_several_identifiers_deduped(self):
        """The same module may hold several handles with different identifiers;
        when one PUBLISH carries both, the shared callback still fires once."""
        calls = queue.Queue()

        def shared(c, userdata, message):
            calls.put(message.payload)

        h1 = self.h.client.subscribe_handle("a/+", shared, identifier=100)
        m1 = self.h.sub_mid_for(100)
        h2 = self.h.client.subscribe_handle("a/b", shared, identifier=200)
        m2 = self.h.sub_mid_for(200)
        self.h.inject(make_suback(m1, [0]))
        self.h.inject(make_suback(m2, [0]))

        self.h.inject(make_publish(
            "a/b", b"once", identifiers=(100, 200)))
        self.assertEqual(calls.get(timeout=5), b"once")
        self.assertTrue(calls.empty())

    def test_partial_rejection_and_activation(self):
        """A failure reason code (0x8F topic filter invalid) leaves the handler
        inactive; a success code activates it and routes messages."""
        got = queue.Queue()
        handle_good = self.h.client.subscribe_handle(
            "good/+", lambda c, u, m: got.put(m.payload), identifier=1)
        mid_good = self.h.sub_mid_for(1)
        handle_bad = self.h.client.subscribe_handle(
            "bad/#", lambda c, u, m: None, identifier=2)
        mid_bad = self.h.sub_mid_for(2)

        self.h.inject(make_suback(mid_good, [0]))
        self.h.inject(make_suback(mid_bad, [143]))  # topic filter invalid

        self.assertIs(handle_good.state, SubscriptionState.ACTIVE)
        self.assertIs(handle_bad.state, SubscriptionState.INACTIVE)
        self.assertEqual(handle_bad.reason_code.value, 143)
        # Still registered (inactive) so the handle can be retried.
        self.assertIn(2, self.h.client._subscription_handles)
        self.assertIn(1, self.h.client._subscription_handles)

        # A later PUBLISH referencing the rejected identifier is dropped.
        self.h.inject(make_publish("bad/x", b"nope", identifiers=(2,)))
        # Accepted identifier still works.
        self.h.inject(make_publish("good/topic", b"ok", identifiers=(1,)))
        self.assertEqual(got.get(timeout=5), b"ok")
        self.assertTrue(got.empty())

    def test_message_before_suback_not_delivered(self):
        """Misbehaving peer: a PUBLISH carrying the subscription identifier
        arrives *before* the SUBACK; the pending handler must not see it.
        A message after the SUBACK is delivered normally."""
        got = queue.Queue()
        handle = self.h.client.subscribe_handle(
            "early/+", lambda c, u, m: got.put(m.payload), identifier=7)
        mid = self.h.sub_mid_for(7)
        self.assertIs(handle.state, SubscriptionState.PENDING)

        self.h.inject(make_publish("early/topic", b"early", identifiers=(7,)))
        self.assertTrue(got.empty())
        self.assertIs(handle.state, SubscriptionState.PENDING)

        self.h.inject(make_suback(mid, [0]))
        self.assertIs(handle.state, SubscriptionState.ACTIVE)

        self.h.inject(make_publish("early/topic", b"late", identifiers=(7,)))
        self.assertEqual(got.get(timeout=5), b"late")
        self.assertTrue(got.empty())

    def test_unsubscribe_revokes_handler(self):
        got = queue.Queue()
        handle = self.h.client.subscribe_handle(
            "rev/me", lambda c, u, m: got.put(m.payload), identifier=3)
        sub_mid = self.h.sub_mid_for(3)
        self.h.inject(make_suback(sub_mid, [0]))
        self.assertIs(handle.state, SubscriptionState.ACTIVE)

        result, unsub_mid = handle.unsubscribe()
        self.assertEqual(result, mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS)
        # Still active immediately: only the UNSUBACK confirms the removal.
        self.assertIs(handle.state, SubscriptionState.ACTIVE)
        self.h.inject(make_publish("rev/me", b"inflight", identifiers=(3,)))
        self.assertEqual(got.get(timeout=5), b"inflight")

        self.h.inject(make_unsuback(unsub_mid, [0]))
        self.assertIs(handle.state, SubscriptionState.INACTIVE)
        self.assertNotIn(3, self.h.client._subscription_handles)
        self.assertEqual(
            self.h.sent_unsubscribes[-1], (unsub_mid, ["rev/me"]))

        # Messages after UNSUBACK are not routed.
        self.h.inject(make_publish("rev/me", b"gone", identifiers=(3,)))
        self.assertTrue(got.empty())

    def test_unsubscribe_failure_keeps_handler(self):
        """An error reason code (>= 0x80) in UNSUBACK means the broker did not
        remove the subscription: the handler stays active."""
        got = queue.Queue()
        handle = self.h.client.subscribe_handle(
            "keep/me", lambda c, u, m: got.put(m.payload), identifier=4)
        sub_mid = self.h.sub_mid_for(4)
        self.h.inject(make_suback(sub_mid, [0]))

        _result, unsub_mid = handle.unsubscribe()
        self.h.inject(make_unsuback(unsub_mid, [0x91]))
        self.assertIs(handle.state, SubscriptionState.ACTIVE)
        self.assertIn(4, self.h.client._subscription_handles)
        self.h.inject(make_publish("keep/me", b"still", identifiers=(4,)))
        self.assertEqual(got.get(timeout=5), b"still")

    def test_retry_after_rejection(self):
        """A rejected handle can be re-armed with resubscribe_handle()."""
        handle = self.h.client.subscribe_handle(
            "retry/+", lambda c, u, m: None, identifier=8)
        first_mid = self.h.sub_mid_for(8)
        self.h.inject(make_suback(first_mid, [143]))
        self.assertIs(handle.state, SubscriptionState.INACTIVE)

        result, second_mid = self.h.client.resubscribe_handle(handle)
        self.assertEqual(result, mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS)
        self.assertIs(handle.state, SubscriptionState.PENDING)
        self.h.inject(make_suback(second_mid, [2]))
        self.assertIs(handle.state, SubscriptionState.ACTIVE)
        self.assertEqual(handle.qos, 2)

    def test_plain_unsubscribe_topic_revokes_handle(self):
        handle = self.h.client.subscribe_handle(
            "legacy/x", lambda c, u, m: None, identifier=5)
        sub_mid = self.h.sub_mid_for(5)
        self.h.inject(make_suback(sub_mid, [0]))

        _result, unsub_mid = self.h.client.unsubscribe("legacy/x")
        self.h.inject(make_unsuback(unsub_mid, [0]))
        self.assertIs(handle.state, SubscriptionState.INACTIVE)
        self.assertNotIn(5, self.h.client._subscription_handles)

    def test_dispatch_order_with_topic_callbacks(self):
        """Both interfaces present:
        1. subscription identifier handlers (deduplicated),
        2. message_callback_add() topic callbacks (trie order),
        3. on_message only when no topic callback matched."""
        order = queue.Queue()

        def record(name):
            def _h(c, userdata, message):
                order.put((name, message.topic))
            return _h

        self.h.client.on_message = record("on_message")
        self.h.client.message_callback_add("both/#", record("topic"))

        h_both = self.h.client.subscribe_handle(
            "both/+", record("handle"), identifier=30)
        h_only = self.h.client.subscribe_handle(
            "only/+", record("only_handle"), identifier=31)
        self.h.inject(make_suback(self.h.sub_mid_for(30), [0]))
        self.h.inject(make_suback(self.h.sub_mid_for(31), [0]))

        self.h.inject(make_publish(
            "both/here", b"1", identifiers=(30, 30)))
        self.h.inject(make_publish("only/handle", b"2", identifiers=(31,)))

        self.assertEqual(order.get(timeout=5),
                         ("handle", "both/here"))
        self.assertEqual(order.get(timeout=5),
                         ("topic", "both/here"))
        self.assertEqual(order.get(timeout=5),
                         ("only_handle", "only/handle"))
        self.assertEqual(order.get(timeout=5),
                         ("on_message", "only/handle"))
        self.assertTrue(order.empty())

    def test_reconnect_stale_suback_not_accepted(self):
        """A SUBACK carrying the mid of the pre-reconnect SUBSCRIBE must not
        activate the handle after reconnect; only a fresh SUBACK after
        resubscribe_handle() does."""
        got = queue.Queue()
        handle = self.h.client.subscribe_handle(
            "reconn/+", lambda c, u, m: got.put(m.payload),
            identifier=42, qos=1)
        old_mid = self.h.sub_mid_for(42)
        self.assertIs(handle.state, SubscriptionState.PENDING)

        # Simulate the network drop + reconnect() reset path.
        self.h.client._reset_pending_subscriptions()
        self.assertIs(handle.state, SubscriptionState.INACTIVE)
        self.assertEqual(dict(self.h.client._pending_subscriptions), {})

        # Misbehaving peer replays a SUBACK with the stale mid.
        self.h.inject(make_suback(old_mid, [0]))
        self.assertIs(handle.state, SubscriptionState.INACTIVE)
        self.assertTrue(got.empty())

        # Re-arm using the same identifier; only the new SUBACK activates.
        result, new_mid = self.h.client.resubscribe_handle(handle)
        self.assertEqual(result, mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS)
        self.assertNotEqual(new_mid, old_mid)
        self.assertIs(handle.state, SubscriptionState.PENDING)
        self.assertIn(42, self.h.client._subscription_handles)

        self.h.inject(make_suback(new_mid, [1]))
        self.assertIs(handle.state, SubscriptionState.ACTIVE)
        self.assertEqual(handle.qos, 1)
        self.h.inject(make_publish("reconn/x", b"fresh", identifiers=(42,)))
        self.assertEqual(got.get(timeout=5), b"fresh")

    def test_reconnect_clears_pending_in_reconnect(self):
        """`reconnect()` itself (the path used by automatic reconnects) drops
        pending SUBACKs and deactivates handles."""
        handle = self.h.client.subscribe_handle(
            "drop/me", lambda c, u, m: None, identifier=9)
        old_mid = self.h.sub_mid_for(9)
        self.assertIn(old_mid, self.h.client._pending_subscriptions)

        # Avoid actual socket creation: monkeypatch the socket creation.
        class FakeSock:
            def close(self):
                pass

            def setblocking(self, flag):
                pass
        self.h.client._create_socket = lambda: FakeSock()
        self.h.client._send_connect = lambda keepalive: (
            mqtt.MQTTErrorCode.MQTT_ERR_SUCCESS)
        self.h.client._call_socket_open = lambda sock: None
        self.h.client._host = "localhost"
        self.h.client._port = 1
        self.h.client.reconnect()

        self.assertEqual(dict(self.h.client._pending_subscriptions), {})
        self.assertIs(handle.state, SubscriptionState.INACTIVE)
        # A stale SUBACK after reconnect is ignored.
        self.h.inject(make_suback(old_mid, [0]))
        self.assertIs(handle.state, SubscriptionState.INACTIVE)

    def test_disconnect_packet_clears_pending(self):
        """A broker-initiated DISCONNECT (or any _sock_close() path) also
        invalidates pending SUBACKs."""
        handle = self.h.client.subscribe_handle(
            "drop/me", lambda c, u, m: None, identifier=11)
        old_mid = self.h.sub_mid_for(11)

        # DISCONNECT packet with reason code 0 (normal), no properties.
        self.h.inject(encode_packet(0xE0, encode_vbi(0)))
        self.assertIs(handle.state, SubscriptionState.INACTIVE)
        self.assertEqual(dict(self.h.client._pending_subscriptions), {})
        # The stale SUBACK is now ignored.
        self.h.inject(make_suback(old_mid, [0]))
        self.assertIs(handle.state, SubscriptionState.INACTIVE)

    def test_auto_identifiers_and_validation(self):
        h1 = self.h.client.subscribe_handle("a/+", lambda c, u, m: None)
        h2 = self.h.client.subscribe_handle("b/+", lambda c, u, m: None)
        self.assertEqual(h1.identifier, 1)
        self.assertEqual(h2.identifier, 2)

        with self.assertRaises(ValueError):
            self.h.client.subscribe_handle(
                "c/+", lambda c, u, m: None, identifier=1)
        with self.assertRaises(ValueError):
            self.h.client.subscribe_handle(
                "d/+", lambda c, u, m: None, identifier=0)
        with self.assertRaises(ValueError):
            self.h.client.subscribe_handle("", lambda c, u, m: None)
        with self.assertRaises(ValueError):
            self.h.client.subscribe_handle("e/+", None)

    def test_v3_client_rejects_handles(self):
        client = mqtt.Client(
            CallbackAPIVersion.VERSION2,
            client_id="v3",
            protocol=mqtt.MQTTv311,
        )
        with self.assertRaises(RuntimeError):
            client.subscribe_handle("v3/+", lambda c, u, m: None)

    def test_legacy_dispatch_unchanged_without_identifiers(self):
        """Messages without a Subscription Identifier keep the exact legacy
        routing: topic callbacks first, on_message only if none matched."""
        topic_box, default_box = queue.Queue(), queue.Queue()
        self.h.client.message_callback_add(
            "matched/+", lambda c, u, m: topic_box.put(m.payload))
        self.h.client.on_message = lambda c, u, m: default_box.put(m.payload)

        # No properties (property length 0), no identifier.
        self.h.inject(make_publish("matched/x", b"1"))
        self.h.inject(make_publish("other/x", b"2"))

        self.assertEqual(topic_box.get(timeout=5), b"1")
        self.assertEqual(default_box.get(timeout=5), b"2")
        self.assertTrue(topic_box.empty())
        self.assertTrue(default_box.empty())

    def test_not_connected_handle_stays_inactive(self):
        """subscribe_handle() while disconnected registers the handle but
        leaves it inactive; it can be armed after reconnecting."""
        client = mqtt.Client(
            CallbackAPIVersion.VERSION2,
            client_id="offline",
            protocol=mqtt.MQTTv5,
        )
        handle = client.subscribe_handle("off/+", lambda c, u, m: None)
        self.assertIs(handle.state, SubscriptionState.INACTIVE)
        self.assertEqual(handle.identifier, 1)


if __name__ == "__main__":
    unittest.main()
