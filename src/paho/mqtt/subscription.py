# Copyright (c) 2024 Roger Light <roger@atchoo.org>
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v2.0
# and Eclipse Distribution License v1.0 which accompany this distribution.
#
# The Eclipse Public License is available at
#    http://www.eclipse.org/legal/epl-v20.html
# and the Eclipse Distribution License is available at
#   http://www.eclipse.org/org/documents/edl-v10.php.

"""MQTT version 5.0 subscription handles.

A :class:`SubscriptionHandle` associates a message callback with an MQTT 5.0
*Subscription Identifier*. The broker echoes the identifier (possibly several of
them) in the PUBLISH packets that match the corresponding subscription, which
allows the client to route a message to the right module even when several
modules subscribe to overlapping topics.

A handle is only "active" once the broker has accepted the subscription in the
SUBACK packet: messages received beforehand (for instance from a misbehaving
peer that publishes before acknowledging the SUBSCRIBE) are never delivered to
the handler.
"""

from __future__ import annotations

import enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .client import Client
    from .reasoncodes import ReasonCode

#: Type of the callback attached to a subscription handle. The signature is the
#: same as ``on_message``: ``callback(client, userdata, message)``.
if TYPE_CHECKING:
    from .client import CallbackOnMessage


class SubscriptionState(enum.Enum):
    """Lifecycle state of a :class:`SubscriptionHandle`."""

    #: The SUBSCRIBE packet has been queued/sent, no (matching) SUBACK has been
    #: received yet.
    PENDING = "pending"
    #: The broker accepted the subscription in a SUBACK packet. Messages
    #: carrying the handle's subscription identifier are delivered to the
    #: handler.
    ACTIVE = "active"
    #: The broker rejected the subscription (SUBACK reason code >= 0x80), the
    #: subscription has been removed through `unsubscribe()`, or the
    #: connection was lost before the SUBACK arrived.
    INACTIVE = "inactive"


class SubscriptionHandle:
    """A subscription identified by an MQTT 5.0 Subscription Identifier.

    Instances are created with :meth:`paho.mqtt.client.Client.subscribe_handle`
    and must not be instantiated directly.

    A handle goes through the states defined in :class:`SubscriptionState`:

    * ``PENDING`` right after the SUBSCRIBE packet has been sent,
    * ``ACTIVE`` once the broker returns a success reason code in SUBACK,
    * ``INACTIVE`` when the broker rejects the subscription, when
      :meth:`unsubscribe` completes successfully, or when the connection is
      lost (including while a SUBSCRIBE is still pending).

    The handler callback is only invoked while the handle is ``ACTIVE``.
    """

    def __init__(
        self,
        client: "Client",
        topic: str,
        identifier: int,
        callback: "CallbackOnMessage",
        qos: int = 0,
    ) -> None:
        self._client = client
        self._topic = topic
        self._identifier = identifier
        self._callback = callback
        self._qos = qos
        self._state = SubscriptionState.PENDING
        #: Reason code returned by the broker in the last SUBACK for this
        #: subscription, or ``None`` before a SUBACK has been processed.
        self.reason_code: "ReasonCode | None" = None

    @property
    def topic(self) -> str:
        """Topic filter of the subscription."""
        return self._topic

    @property
    def identifier(self) -> int:
        """Subscription Identifier associated with the handle."""
        return self._identifier

    @property
    def callback(self) -> "CallbackOnMessage":
        """Message callback invoked for matching messages while active."""
        return self._callback

    @property
    def qos(self) -> int:
        """QoS granted by the broker, once the handle is active."""
        return self._qos

    @property
    def state(self) -> SubscriptionState:
        """Current lifecycle state of the handle."""
        return self._state

    def is_active(self) -> bool:
        """Return ``True`` when messages are currently routed to the handler."""
        return self._state is SubscriptionState.ACTIVE

    def unsubscribe(self, properties: Any = None) -> Any:
        """Remove the subscription associated with this handle.

        The handler stays active until the broker acknowledges the removal in
        the UNSUBACK packet. It is a shortcut for
        ``client.unsubscribe_handle(handle, properties)``.

        :return: the ``(result, mid)`` tuple returned by
            :meth:`paho.mqtt.client.Client.unsubscribe`.
        """
        return self._client.unsubscribe_handle(self, properties=properties)

    def __repr__(self) -> str:
        return (
            f"SubscriptionHandle(topic={self._topic!r}, "
            f"identifier={self._identifier}, state={self._state.value})"
        )
