"""Pluggable persistent session state for the MQTT client.

The :class:`~paho.mqtt.client.Client` keeps the state of the in-progress
QoS 1/2 protocol exchanges in memory:

* outgoing QoS 1/2 messages (and queued messages) in
  ``Client._out_messages``,
* incoming QoS 2 messages that are between PUBREC and PUBREL in
  ``Client._in_messages``,
* the last allocated packet id in ``Client._last_mid``.

A :class:`SessionStore` persists this state so that a *new* client process
may finish the exchanges after a restart instead of silently dropping them
or, worse, reusing a packet id that is still part of a handshake with the
broker.

What this does and does not guarantee
-------------------------------------

Persisting this state makes the **protocol level** exchanges resumable: an
outgoing QoS 1 message is retransmitted, an outgoing QoS 2 exchange resumes
at PUBLISH or PUBREL as required, and an incoming QoS 2 exchange resumes at
PUBREC/PUBCOMP.

It does **not** provide exactly-once delivery of application side effects:

* An outgoing message is persisted before it has been acknowledged. If the
  crash happens after the broker accepted it but before the acknowledgment
  was processed, the message is sent again with the DUP flag set
  (at-least-once).
* For an incoming QoS 2 message, the application callback runs on PUBREL.
  If the crash happens after PUBREL was received but before PUBCOMP reached
  the broker, the broker re-sends PUBREL and the callback runs again after a
  restart.

Applications that cannot tolerate a duplicated side effect must make their
handlers idempotent.
"""
from __future__ import annotations

import abc
import base64
import collections
import contextlib
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .client import Client
    from .client import MQTTMessage as MQTTMessageType
    from .properties import Properties

# Magic prefix and on-disk format version of FileSessionStore.
_FILE_MAGIC = b"PAHOMQSS"
_FILE_VERSION = 1
# Layout: magic (8B) | version (1B) | payload length (4B big endian) |
#         sha256(payload) (32B) | payload
_HEADER_SIZE = 8 + 1 + 4 + 32


class CorruptSessionError(Exception):
    """Raised when a session store cannot be read safely.

    Truncated files, failed checksum verification, unknown format versions
    and payloads that cannot be decoded all lead to this exception. A client
    that receives it must assume that no part of the stored session is
    trustworthy and start a clean session.
    """


@dataclass(slots=True)
class PersistentMessage:
    """Serializable subset of :class:`paho.mqtt.client.MQTTMessage`.

    Runtime-only data (delivery timestamps, the blocking
    :class:`paho.mqtt.client.MQTTMessageInfo` handle) is intentionally not
    part of the persistent state.
    """

    mid: int
    topic: bytes
    payload: bytes
    qos: int
    retain: bool = False
    dup: bool = False
    state: int = 0
    properties: "Properties | None" = None


@dataclass(slots=True)
class SessionState:
    """The resumable session state of one client."""

    out_messages: dict[int, PersistentMessage] = field(default_factory=dict)
    """Outgoing QoS 1/2 (and queued) messages keyed by packet id."""

    in_messages: dict[int, PersistentMessage] = field(default_factory=dict)
    """Incoming QoS 2 messages between PUBREC and PUBREL keyed by packet id."""

    last_mid: int = 0
    """High-water mark of allocated packet ids (1..65535)."""


class SessionStore(abc.ABC):
    """Abstract pluggable session state store.

    Implementations must be safe to call from the client's network loop
    thread; the file reference implementation performs synchronous I/O.
    All methods are called with the client's internal message locks
    released.
    """

    @abc.abstractmethod
    def load(self, client_id: str) -> SessionState | None:
        """Return the previously stored state, or ``None`` if no state exists.

        :raises CorruptSessionError: if the stored state cannot be decoded or
            fails its integrity checks. The caller treats this identically to
            "no usable state" and starts a clean session rather than risking a
            half-restored handshake.
        """

    @abc.abstractmethod
    def save(self, client_id: str, state: SessionState) -> None:
        """Durably replace the stored state for ``client_id``.

        A crash during ``save`` must leave either the previous complete state
        or the new complete state on disk, never a torn file.

        :raises OSError: if the state cannot be written.
        """

    @abc.abstractmethod
    def delete(self, client_id: str) -> None:
        """Remove the stored state (e.g. when a clean session is started).

        A missing state must not raise.
        """


def _message_to_dict(message: PersistentMessage) -> dict[str, Any]:
    data: dict[str, Any] = {
        "mid": message.mid,
        "topic": base64.b64encode(message.topic).decode("ascii"),
        "payload": base64.b64encode(bytes(message.payload)).decode("ascii"),
        "qos": message.qos,
        "retain": message.retain,
        "dup": message.dup,
        "state": int(message.state),
    }
    if message.properties is not None:
        data["properties"] = base64.b64encode(
            message.properties.pack()
        ).decode("ascii")
    return data


def _message_from_dict(
    data: dict[str, Any], packet_type: int
) -> PersistentMessage:
    # Imported lazily to keep this module importable without the client and
    # to avoid an import cycle.
    from .properties import Properties

    properties = None
    packed_properties = data.get("properties")
    if packed_properties is not None:
        properties, _ = Properties(packet_type).unpack(
            base64.b64decode(packed_properties.encode("ascii"))
        )

    return PersistentMessage(
        mid=int(data["mid"]),
        topic=base64.b64decode(data["topic"].encode("ascii")),
        payload=base64.b64decode(data["payload"].encode("ascii")),
        qos=int(data["qos"]),
        retain=bool(data.get("retain", False)),
        dup=bool(data.get("dup", False)),
        state=int(data.get("state", 0)),
        properties=properties,
    )


def state_to_dict(state: SessionState) -> dict[str, Any]:
    """Serialise a :class:`SessionState` to plain JSON-compatible data."""
    # Incoming QoS 2 messages are PUBLISH packets.
    return {
        "version": 1,
        "last_mid": int(state.last_mid),
        "out_messages": [
            _message_to_dict(m) for m in state.out_messages.values()
        ],
        "in_messages": [
            _message_to_dict(m) for m in state.in_messages.values()
        ],
    }


def state_from_dict(data: Any) -> SessionState:
    """Rebuild a :class:`SessionState` from JSON-compatible data.

    :raises CorruptSessionError: if the data is malformed.
    """
    if not isinstance(data, dict):
        raise CorruptSessionError("session payload is not an object")

    state = SessionState()
    try:
        state.last_mid = int(data["last_mid"])
        if not 0 <= state.last_mid <= 65535:
            raise CorruptSessionError("last_mid out of range")

        for bucket, packet_type in (
            ("out_messages", 3),  # PUBLISH
            ("in_messages", 3),
        ):
            entries = data.get(bucket)
            if not isinstance(entries, list):
                raise CorruptSessionError(f"{bucket} is not a list")
            target: dict[int, PersistentMessage] = {}
            for entry in entries:
                if not isinstance(entry, dict):
                    raise CorruptSessionError("message entry is not an object")
                message = _message_from_dict(entry, packet_type)
                if not 0 < message.mid <= 65535:
                    raise CorruptSessionError("message mid out of range")
                if message.qos not in (1, 2):
                    raise CorruptSessionError("message qos out of range")
                if not 0 <= message.state <= 9:
                    raise CorruptSessionError("message state out of range")
                if message.mid in target:
                    raise CorruptSessionError("duplicate message mid")
                target[message.mid] = message
            if bucket == "out_messages":
                state.out_messages = target
            else:
                state.in_messages = target
    except CorruptSessionError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise CorruptSessionError(f"malformed session payload: {exc}") from exc

    return state


class FileSessionStore(SessionStore):
    """Reference :class:`SessionStore` keeping one JSON file per client id.

    Each file starts with a fixed header followed by a SHA-256 checksummed
    payload, so truncated or otherwise corrupted writes are detected on the
    next load instead of being silently restored. Writes are atomic (write to
    a temporary file in the same directory, ``fsync`` and ``os.replace``).

    The store may be shared by several :class:`~paho.mqtt.client.Client`
    instances as long as they use distinct client ids; the file name is the
    SHA-256 digest of the UTF-8 encoded client id, which keeps path
    separators and other special characters out of the file system name.
    """

    def __init__(self, directory: str) -> None:
        self._directory = directory

    def _path(self, client_id: str) -> str:
        # Client ids are UTF-8 and may contain path separators; use the hex
        # of the id bytes for a safe, collision-free file name.
        digest = hashlib.sha256(client_id.encode("utf-8")).hexdigest()
        return os.path.join(self._directory, f"mqtt-session-{digest}.json")

    def load(self, client_id: str) -> SessionState | None:
        path = self._path(client_id)
        try:
            with open(path, "rb") as handle:
                blob = handle.read()
        except FileNotFoundError:
            return None

        if len(blob) < _HEADER_SIZE:
            raise CorruptSessionError("session file is truncated")
        if blob[:8] != _FILE_MAGIC:
            raise CorruptSessionError("bad session file magic")
        version = blob[8]
        if version != _FILE_VERSION:
            raise CorruptSessionError(
                f"unsupported session file version {version}"
            )
        payload_length = int.from_bytes(blob[9:13], "big")
        checksum = blob[13:45]
        payload = blob[_HEADER_SIZE:]
        if len(payload) != payload_length:
            raise CorruptSessionError("session file length mismatch")
        if hashlib.sha256(payload).digest() != checksum:
            raise CorruptSessionError("session file checksum mismatch")

        try:
            data = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CorruptSessionError(f"invalid session payload: {exc}") from exc
        return state_from_dict(data)

    def save(self, client_id: str, state: SessionState) -> None:
        payload = json.dumps(
            state_to_dict(state), separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        checksum = hashlib.sha256(payload).digest()
        header = (
            _FILE_MAGIC
            + bytes((_FILE_VERSION,))
            + len(payload).to_bytes(4, "big")
            + checksum
        )

        os.makedirs(self._directory, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=".mqtt-session-", dir=self._directory
        )
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(header)
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self._path(client_id))
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp_name)
            raise

    def delete(self, client_id: str) -> None:
        try:
            os.unlink(self._path(client_id))
        except FileNotFoundError:
            pass


def apply_state_to_client(client: "Client", state: SessionState) -> None:
    """Restore a loaded :class:`SessionState` into a freshly created client.

    This replaces the client's in-memory message tables and packet id
    counter; the regular reconnect logic then re-arms retransmissions
    (timestamps are zeroed and message states are reset as appropriate).
    """
    out_messages: collections.OrderedDict[int, MQTTMessageType] = collections.OrderedDict()
    in_messages: collections.OrderedDict[int, MQTTMessageType] = collections.OrderedDict()

    for persistent in state.out_messages.values():
        out_messages[persistent.mid] = _persistent_to_message(persistent)
    for persistent in state.in_messages.values():
        in_messages[persistent.mid] = _persistent_to_message(persistent)

    client._out_messages = out_messages
    client._in_messages = in_messages
    client._inflight_messages = 0
    client._last_mid = state.last_mid


def _persistent_to_message(persistent: PersistentMessage) -> MQTTMessageType:
    from .client import MQTTMessage
    message = MQTTMessage(persistent.mid, persistent.topic)
    message.payload = persistent.payload
    message.qos = persistent.qos
    message.retain = persistent.retain
    message.dup = persistent.dup
    message.state = persistent.state
    message.properties = persistent.properties
    message.timestamp = 0.0
    return message


def snapshot_client(client: "Client") -> SessionState:
    """Build a :class:`SessionState` from a connected client's live state."""
    with client._out_message_mutex:
        out_messages = {
            mid: _message_to_persistent(message)
            for mid, message in client._out_messages.items()
        }
    with client._in_message_mutex:
        in_messages = {
            mid: _message_to_persistent(message)
            for mid, message in client._in_messages.items()
        }
    return SessionState(
        out_messages=out_messages,
        in_messages=in_messages,
        last_mid=client._last_mid,
    )


def _message_to_persistent(message: Any) -> PersistentMessage:
    return PersistentMessage(
        mid=message.mid,
        topic=bytes(message._topic),
        payload=bytes(message.payload),
        qos=message.qos,
        retain=message.retain,
        dup=message.dup,
        state=int(message.state),
        properties=message.properties,
    )
