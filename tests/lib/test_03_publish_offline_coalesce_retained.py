# Test whether offline coalescing of retained publishes keeps only the last
# retained state per topic, in queue order, without touching non-retained
# messages. An empty payload retained publish (retained delete) is a valid
# final state.


import tests.paho_test as paho_test

connect_packet = paho_test.gen_connect("offline-coalesce-retained", keepalive=60)
connack_packet = paho_test.gen_connack(rc=0)

disconnect_packet = paho_test.gen_disconnect()

# a1 (mid 1) and b1 (mid 2) are superseded while offline and must never
# appear on the wire.
publish_n1_packet = paho_test.gen_publish("coalesce/n", qos=1, mid=3, payload="n1")
publish_a2_packet = paho_test.gen_publish("coalesce/a", qos=1, mid=4, payload="a2", retain=True)
publish_b2_packet = paho_test.gen_publish("coalesce/b", qos=1, mid=5, retain=True)

puback3_packet = paho_test.gen_puback(3)
puback4_packet = paho_test.gen_puback(4)
puback5_packet = paho_test.gen_puback(5)


def test_03_publish_offline_coalesce_retained(server_socket, start_client):
    start_client("03-publish-offline-coalesce-retained.py")

    (conn, address) = server_socket.accept()
    conn.settimeout(15)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    # Only the final state of each topic is sent, in queue order, with the
    # non-retained message left untouched.
    paho_test.expect_packet(conn, "publish n1", publish_n1_packet)
    conn.send(puback3_packet)

    paho_test.expect_packet(conn, "publish a2", publish_a2_packet)
    conn.send(puback4_packet)

    paho_test.expect_packet(conn, "publish b2 (retained delete)", publish_b2_packet)
    conn.send(puback5_packet)

    paho_test.expect_packet(conn, "disconnect", disconnect_packet)

    conn.close()
