# Copyright (c) 2026 Roger Light and others
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v2.0
# and Eclipse Distribution License v1.0 which accompany this distribution.
#
# The Eclipse Public License is available at
#    http://www.eclipse.org/legal/epl-v20.html
# and the Eclipse Distribution License is available at
#   http://www.eclipse.org/org/documents/edl-v10.php.
"""
Automatic MQTT v5 topic alias management for outgoing PUBLISH packets.

A topic alias mapping is only valid for the lifetime of a single network
connection, so the Client resets the manager on every successful (re)connect.
"""
from __future__ import annotations

import collections

from .packettypes import PacketTypes
from .properties import Properties


class TopicAliasManager:
    """Manage outgoing MQTT v5 topic aliases for a single network connection.

    ``maximum`` is the number of aliases the application wants to use.  The
    effective limit is ``min(maximum, broker_maximum)`` where
    ``broker_maximum`` is the Topic Alias Maximum sent by the broker in the
    CONNACK (0 if the broker did not send one, meaning aliases are forbidden).

    The mapping maintained by this class is a bijection between topics and
    aliases.  Rules applied by `apply()`:

    * The first time a topic is seen, the full topic is sent together with a
      newly allocated alias.
    * If the topic already has an alias, the topic name is omitted (empty
      topic) and only the alias is sent.
    * If no alias is free, the least recently used alias is reassigned and
      the full topic is sent again (an alias replacement must always carry
      its topic).
    * If the application explicitly sets the ``TopicAlias`` property, the
      explicit value always wins: the packet is sent with the full topic and
      the manager adopts the explicit mapping, dropping any conflicting
      automatic assignment.  An explicit alias outside the effective limit
      raises ``ValueError``.
    """

    def __init__(self, maximum: int = 10):
        if maximum < 1 or maximum > 65535:
            raise ValueError("maximum must be in the range 1-65535")
        self._maximum = maximum
        self._broker_maximum = 0
        self._topic_to_alias: dict[bytes, int] = {}
        # Aliases ordered by last use, least recently used first.
        self._alias_to_topic: collections.OrderedDict[int, bytes] = collections.OrderedDict()

    @property
    def maximum(self) -> int:
        """Effective number of aliases usable on the current connection."""
        return min(self._maximum, self._broker_maximum)

    @property
    def broker_maximum(self) -> int:
        """Topic Alias Maximum received from the broker (0 if none)."""
        return self._broker_maximum

    def reset(self, broker_maximum: int = 0) -> None:
        """Forget all mappings.

        Must be called on every new network connection because topic aliases
        are only valid for the lifetime of the connection.  ``broker_maximum``
        is the Topic Alias Maximum from the CONNACK properties.
        """
        self._broker_maximum = broker_maximum
        self._topic_to_alias.clear()
        self._alias_to_topic.clear()

    def apply(
        self, topic: bytes, properties: Properties | None
    ) -> tuple[bytes, Properties | None]:
        """Compute the (topic, properties) to put on the wire for a PUBLISH.

        ``topic`` must be the full, non-empty topic.  Returns the topic and
        properties to actually send; the caller's ``properties`` object is
        never modified.
        """
        explicit_alias = None
        if properties is not None and hasattr(properties, "TopicAlias"):
            explicit_alias = properties.TopicAlias

        if explicit_alias is not None:
            if explicit_alias > self.maximum:
                raise ValueError(
                    f"TopicAlias {explicit_alias} exceeds the usable topic "
                    f"alias maximum of {self.maximum}"
                )
            # Explicit aliases always win: send the full topic so the broker
            # (re)binds the alias, and adopt the mapping ourselves.
            self._bind(topic, explicit_alias)
            return topic, properties

        if self.maximum < 1:
            return topic, properties

        alias = self._topic_to_alias.get(topic)
        if alias is not None:
            self._alias_to_topic.move_to_end(alias)
            return b"", self._properties_with_alias(properties, alias)

        if len(self._alias_to_topic) >= self.maximum:
            # Reassign the least recently used alias; the full topic must be
            # sent again because the alias now refers to a different topic.
            alias, old_topic = self._alias_to_topic.popitem(last=False)
            del self._topic_to_alias[old_topic]
        else:
            alias = self._allocate_alias()

        self._bind(topic, alias)
        return topic, self._properties_with_alias(properties, alias)

    def _allocate_alias(self) -> int:
        for alias in range(1, self.maximum + 1):
            if alias not in self._alias_to_topic:
                return alias
        raise RuntimeError("no topic alias available")  # pragma: no cover

    def _bind(self, topic: bytes, alias: int) -> None:
        old_alias = self._topic_to_alias.get(topic)
        if old_alias is not None and old_alias != alias:
            self._alias_to_topic.pop(old_alias, None)
        old_topic = self._alias_to_topic.get(alias)
        if old_topic is not None and old_topic != topic:
            self._topic_to_alias.pop(old_topic, None)
        self._topic_to_alias[topic] = alias
        self._alias_to_topic[alias] = topic
        self._alias_to_topic.move_to_end(alias)

    @staticmethod
    def _properties_with_alias(properties: Properties | None, alias: int) -> Properties:
        # Never mutate the caller's Properties object: it may be reused for
        # further publishes.
        new_properties = Properties(PacketTypes.PUBLISH)
        if properties is not None:
            for name in properties.names:
                compressed_name = name.replace(" ", "")
                if hasattr(properties, compressed_name):
                    setattr(new_properties, compressed_name, getattr(properties, compressed_name))
        new_properties.TopicAlias = alias
        return new_properties
