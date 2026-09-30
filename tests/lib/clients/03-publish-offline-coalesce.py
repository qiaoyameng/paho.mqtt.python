import paho.mqtt.client as mqtt

from tests.paho_test import get_test_server_port, loop_until_keyboard_interrupt

first_connect = True
coalesced_infos = []
published_mids = set()
# mid 1: in-flight QoS 2 "d1"; mids 2-4: superseded offline; mids 5-9: sent
# after reconnect.
final_mids = {1, 5, 6, 7, 8, 9}


def on_connect(mqttc, obj, flags, rc):
    global first_connect
    assert rc == 0, f"Connect failed ({rc})"
    if first_connect:
        first_connect = False
        # QoS 2 retained message that will be mid-handshake (waiting for
        # PUBCOMP) when the server drops the connection: it must NOT be
        # coalesced by the offline batch published in on_disconnect.
        mqttc.publish("dev/d", "d1", qos=2, retain=True)


def on_disconnect(mqttc, obj, rc):
    if rc == 0:
        return
    # Offline: queue an interleaved batch, then reconnect.
    coalesced_infos.append(mqttc.publish("dev/a", "a1", qos=1, retain=True))  # mid 2, superseded by mid 4
    coalesced_infos.append(mqttc.publish("dev/b", "b1", qos=1, retain=True))  # mid 3, superseded by mid 5
    coalesced_infos.append(mqttc.publish("dev/a", "a2", qos=1, retain=True))  # mid 4, superseded by mid 8
    mqttc.publish("dev/b", "", qos=1, retain=True)   # mid 5: retained delete, final state of dev/b
    mqttc.publish("dev/c", "c1", qos=1)              # mid 6: non-retained, must be kept
    mqttc.publish("dev/c", "c2", qos=1)              # mid 7: non-retained, must be kept
    mqttc.publish("dev/a", "a3", qos=2, retain=True)  # mid 8: final state of dev/a
    mqttc.publish("dev/d", "d2", qos=1, retain=True)  # mid 9: must not replace in-flight mid 1
    mqttc.reconnect()


def on_publish(mqttc, obj, mid):
    published_mids.add(mid)
    if published_mids == final_mids:
        # Superseded messages must have a queryable terminal state and must
        # not wait forever.
        for info in coalesced_infos:
            assert info.is_published()
            info.wait_for_publish(1)
        mqttc.disconnect()


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "offline-coalesce-test", clean_session=False)
mqttc.retained_offline_coalescing = True
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_publish = on_publish

mqttc.connect("localhost", get_test_server_port())
loop_until_keyboard_interrupt(mqttc)
