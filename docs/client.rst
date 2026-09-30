client module
=============

..
   exclude-members exclude decorator for callback, because decorator are
   documented in there respective on_XXX

.. automodule:: paho.mqtt.client
   :members:
   :exclude-members: connect_callback, connect_fail_callback, disconnect_callback, log_callback,
      message_callback, topic_callback, pre_connect_callback, publish_callback,
      socket_close_callback, socket_open_callback, socket_register_write_callback,
      socket_unregister_write_callback, subscribe_callback, unsubscribe_callback
   :undoc-members:

Subscription handles (MQTT v5.0)
--------------------------------

When several application modules subscribe to overlapping topic filters, a
single incoming message may match more than one subscription. MQTT v5.0 solves
the routing problem with *Subscription Identifiers*: a SUBSCRIBE packet
carries an identifier and the broker echoes it in every matching PUBLISH.

Use :meth:`paho.mqtt.client.Client.subscribe_handle` to create a
:class:`paho.mqtt.subscription.SubscriptionHandle` bound to a message
callback::

    def on_module_message(client, userdata, message):
        ...

    handle = client.subscribe_handle("sensors/+/temp", on_module_message,
                                     identifier=7)

The handler is only activated once the broker returns a success reason code in
the SUBACK packet; before that (including messages a misbehaving peer sends
before acknowledging the SUBSCRIBE) it stays in the ``PENDING`` state and
receives nothing. A failure reason code leaves the handle ``INACTIVE``, as do
a confirmed `unsubscribe()` (or
:meth:`paho.mqtt.client.Client.unsubscribe_handle`) and a lost connection.
After reconnecting, re-arm an existing handle from the ``on_connect`` callback
with :meth:`paho.mqtt.client.Client.resubscribe_handle`; a SUBACK from the old
connection can never activate it.

When a PUBLISH carries several Subscription Identifiers, each matching handle
is invoked at most once, in the order in which the broker returned the
identifiers.

If a handle callback and the legacy callbacks coexist, the dispatch order for
each received message is:

1. Subscription Identifier handlers (deduplicated),
2. topic callbacks registered with
   :meth:`paho.mqtt.client.Client.message_callback_add`,
3. the global ``on_message`` callback, but only when no topic callback from
   step 2 matched the message.

The two interfaces are independent: routing a message to a handle does not
prevent it from also being delivered to the topic callbacks/``on_message``.

.. automodule:: paho.mqtt.subscription
   :members:
   :undoc-members:
