import paho.mqtt.client as mqtt

from tests.paho_test import get_test_server_port, loop_until_keyboard_interrupt

published_mids = []

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "offline-coalesce-retained")
mqttc.coalesce_offline_retained = True

# Queued while offline: only the last retained state per topic must be sent.
info_a1 = mqttc.publish("coalesce/a", "a1", qos=1, retain=True)
info_b1 = mqttc.publish("coalesce/b", "b1", qos=1, retain=True)
info_n1 = mqttc.publish("coalesce/n", "n1", qos=1)
info_a2 = mqttc.publish("coalesce/a", "a2", qos=1, retain=True)
info_b2 = mqttc.publish("coalesce/b", "", qos=1, retain=True)

# Superseded publishes must reach a terminal state instead of waiting forever.
assert info_a1.is_published(), "superseded publish a1 has no terminal state"
assert info_b1.is_published(), "superseded publish b1 has no terminal state"
info_a1.wait_for_publish(1)
info_b1.wait_for_publish(1)

# Everything else must still be pending (queued offline, not yet sent).
assert info_n1.rc == mqtt.MQTT_ERR_NO_CONN
assert info_a2.rc == mqtt.MQTT_ERR_NO_CONN
assert info_b2.rc == mqtt.MQTT_ERR_NO_CONN


def on_publish(mqttc, obj, mid):
    published_mids.append(mid)
    if len(published_mids) == 3:
        assert published_mids == [info_n1.mid, info_a2.mid, info_b2.mid], f"Invalid order: {published_mids}"
        mqttc.disconnect()


mqttc.on_publish = on_publish

mqttc.connect("localhost", get_test_server_port())
loop_until_keyboard_interrupt(mqttc)
