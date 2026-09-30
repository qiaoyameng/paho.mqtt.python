# Test that retained messages queued while offline are coalesced to the last
# state per topic, while non-retained messages and an in-flight QoS 2
# message are never replaced. Checks the exact wire packet order across a
# reconnect with interleaved topics.

import tests.paho_test as paho_test

connect_packet = paho_test.gen_connect(
    "offline-coalesce-test", keepalive=60, clean_session=False,
)
connack_packet = paho_test.gen_connack(rc=0)

disconnect_packet = paho_test.gen_disconnect()

# First connection: QoS 2 retained publish left mid-handshake.
publish_d1 = paho_test.gen_publish("dev/d", qos=2, mid=1, payload="d1", retain=True)
pubrec_1 = paho_test.gen_pubrec(1)
pubrel_1 = paho_test.gen_pubrel(1)
pubcomp_1 = paho_test.gen_pubcomp(1)

# Second connection: the coalesced offline backlog, in queue order. The
# superseded retained publishes (mid 2, 3, 4) must never appear.
publish_b_delete = paho_test.gen_publish("dev/b", qos=1, mid=5, payload="", retain=True)
publish_c1 = paho_test.gen_publish("dev/c", qos=1, mid=6, payload="c1")
publish_c2 = paho_test.gen_publish("dev/c", qos=1, mid=7, payload="c2")
publish_a3 = paho_test.gen_publish("dev/a", qos=2, mid=8, payload="a3", retain=True)
publish_d2 = paho_test.gen_publish("dev/d", qos=1, mid=9, payload="d2", retain=True)
pubrec_8 = paho_test.gen_pubrec(8)
pubrel_8 = paho_test.gen_pubrel(8)
pubcomp_8 = paho_test.gen_pubcomp(8)
pubacks = (
    paho_test.gen_puback(5)
    + paho_test.gen_puback(6)
    + paho_test.gen_puback(7)
    + paho_test.gen_puback(9)
)


def test_03_publish_offline_coalesce(server_socket, start_client):
    start_client("03-publish-offline-coalesce.py")

    (conn, address) = server_socket.accept()
    conn.settimeout(15)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "publish d1", publish_d1)
    conn.send(pubrec_1)
    paho_test.expect_packet(conn, "pubrel", pubrel_1)
    # Drop the connection in the middle of the QoS 2 handshake.
    conn.close()

    (conn, address) = server_socket.accept()
    conn.settimeout(15)

    paho_test.expect_packet(conn, "reconnect", connect_packet)
    conn.send(connack_packet)

    # The in-flight QoS 2 message is resumed, not replaced...
    paho_test.expect_packet(conn, "resend pubrel", pubrel_1)
    conn.send(pubcomp_1)

    # ...then the coalesced backlog is sent in queue order: only the last
    # retained state per topic, non-retained messages untouched.
    paho_test.expect_packet(conn, "retained delete dev/b", publish_b_delete)
    paho_test.expect_packet(conn, "non-retained c1", publish_c1)
    paho_test.expect_packet(conn, "non-retained c2", publish_c2)
    paho_test.expect_packet(conn, "publish a3", publish_a3)
    paho_test.expect_packet(conn, "publish d2", publish_d2)
    conn.send(pubacks + pubrec_8)

    paho_test.expect_packet(conn, "pubrel a3", pubrel_8)
    conn.send(pubcomp_8)

    paho_test.expect_packet(conn, "disconnect", disconnect_packet)

    conn.close()
