import paho.mqtt.client as mqtt

from tests.paho_test import get_test_server_port, loop_until_keyboard_interrupt

first_info = None
second_info = None
published_mids = []

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "offline-coalesce-qos2", clean_session=False)
mqttc.coalesce_offline_retained = True


def on_connect(mqttc, obj, flags, rc):
    global first_info
    assert rc == 0, f"Connect failed ({rc})"
    if first_info is None:
        first_info = mqttc.publish("coalesce/qos2", "first", qos=2, retain=True)


def on_disconnect(mqttc, obj, rc):
    global second_info
    if rc != mqtt.MQTT_ERR_SUCCESS:
        if second_info is None:
            # The QoS 2 publish above is mid-handshake: queueing a newer
            # retained state for the same topic must not replace it.
            second_info = mqttc.publish("coalesce/qos2", "second", qos=1, retain=True)
            assert not first_info.is_published(), "in-flight QoS 2 publish was superseded"
            assert second_info.rc == mqtt.MQTT_ERR_NO_CONN
        mqttc.reconnect()


def on_publish(mqttc, obj, mid):
    published_mids.append(mid)
    if len(published_mids) == 2:
        assert published_mids == [first_info.mid, second_info.mid], f"Invalid order: {published_mids}"
        mqttc.disconnect()


mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_publish = on_publish

mqttc.connect("localhost", get_test_server_port())
loop_until_keyboard_interrupt(mqttc)
