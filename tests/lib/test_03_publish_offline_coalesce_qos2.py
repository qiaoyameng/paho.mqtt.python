# Test whether offline coalescing of retained publishes never replaces an
# in-flight QoS 2 message: the connection drops in the middle of the QoS 2
# handshake, a newer retained state for the same topic is queued while
# offline, and both must be sent after reconnect.


import tests.paho_test as paho_test

connect_packet = paho_test.gen_connect(
    "offline-coalesce-qos2", keepalive=60, clean_session=False,
)
connack_packet = paho_test.gen_connack(rc=0)

disconnect_packet = paho_test.gen_disconnect()

publish_first_packet = paho_test.gen_publish(
    "coalesce/qos2", qos=2, mid=1, payload="first", retain=True)
publish_first_dup_packet = paho_test.gen_publish(
    "coalesce/qos2", qos=2, mid=1, payload="first", retain=True, dup=True)
publish_second_packet = paho_test.gen_publish(
    "coalesce/qos2", qos=1, mid=2, payload="second", retain=True)

pubrec_packet = paho_test.gen_pubrec(1)
pubrel_packet = paho_test.gen_pubrel(1)
pubcomp_packet = paho_test.gen_pubcomp(1)
puback_packet = paho_test.gen_puback(2)


def test_03_publish_offline_coalesce_qos2(server_socket, start_client):
    start_client("03-publish-offline-coalesce-qos2.py")

    (conn, address) = server_socket.accept()
    conn.settimeout(15)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "publish first", publish_first_packet)
    # Drop the connection in the middle of the QoS 2 handshake.
    conn.close()

    (conn, address) = server_socket.accept()
    conn.settimeout(15)

    paho_test.expect_packet(conn, "reconnect", connect_packet)
    conn.send(connack_packet)

    # The in-flight QoS 2 message is retransmitted, not coalesced away, and
    # the newer retained state queued while offline is sent afterwards.
    paho_test.expect_packet(conn, "retransmitted publish first", publish_first_dup_packet)
    paho_test.expect_packet(conn, "publish second", publish_second_packet)

    conn.send(pubrec_packet)
    paho_test.expect_packet(conn, "pubrel", pubrel_packet)
    conn.send(pubcomp_packet)

    conn.send(puback_packet)

    paho_test.expect_packet(conn, "disconnect", disconnect_packet)

    conn.close()
