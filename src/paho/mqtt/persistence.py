"""Pluggable session-state persistence for QoS 1/2 exchanges.

This module lets a `paho.mqtt.client.Client` survive a process restart
without losing track of in-flight QoS 1/2 exchanges. A `SessionStore`
saves, for each client:

* outbound QoS 1/2 messages that have not been fully acknowledged,
* inbound QoS 2 messages for which a PUBREC has been sent but the
  matching PUBREL has not been processed yet,
* the last used packet id, so that packet identifiers of messages that
  are still in a handshake are never reused after a restart.

`FileSessionStore` is the reference implementation backed by a single
local file. Writes are atomic (write to a temporary file, fsync, then
rename) and every snapshot carries a SHA-256 checksum so that a torn or
corrupted write is detected on load and reported as
`SessionStoreCorrupted` instead of being silently trusted.

Important caveat: this guarantees resumption of the MQTT protocol
exchanges (the client retransmits PUBLISH/PUBREL as required by the
spec). It does NOT provide exactly-once semantics for application side
effects: a message that was delivered to the application just before a
crash may be delivered again after recovery, and `on_publish` may not
fire for a message whose acknowledgement was lost. Applications must
remain idempotent.
"""
from __future__ import annotations

import abc
import base64
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "FileSessionStore",
    "SessionSnapshot",
    "SessionStore",
    "SessionStoreCorrupted",
    "SessionStoreError",
    "StoredMessage",
]

_MAGIC = b"PAHOSESS"
_FORMAT_VERSION = 1
_DIGEST_LEN = hashlib.sha256().digest_size


class SessionStoreError(Exception):
    """Base class for session store errors."""


class SessionStoreCorrupted(SessionStoreError):
    """Raised when the persisted session state fails integrity checks.

    This indicates a torn or corrupted write (or external tampering).
    The safe reaction is to discard the file (or restore a backup) and
    start with a clean session; the exception is raised precisely so
    that corrupted state is never silently used.
    """


@dataclass
class StoredMessage:
    """A single message that is part of an unfinished QoS 1/2 exchange."""

    direction: str
    """"out" for messages the client publishes, "in" for inbound QoS 2."""

    mid: int
    """The packet identifier of the message."""

    topic: bytes
    payload: bytes
    qos: int
    retain: bool
    dup: bool
    state: int
    """The `paho.mqtt.enums.MessageState` value of the exchange."""


@dataclass
class SessionSnapshot:
    """A consistent view of everything needed to resume a session."""

    last_mid: int = 0
    """The last packet id handed out by the client."""

    messages: list[StoredMessage] = field(default_factory=list)
    """All messages with an unfinished QoS 1/2 exchange."""


class SessionStore(abc.ABC):
    """Abstract interface for pluggable session-state storage.

    Implementations must guarantee that `save` is atomic: a crash at
    any point must leave either the previous or the new snapshot
    readable, never a mixture. Corrupted data must be reported by
    raising `SessionStoreCorrupted` from `load`.
    """

    @abc.abstractmethod
    def load(self) -> SessionSnapshot:
        """Return the persisted snapshot, or an empty one if none exists.

        :raises SessionStoreCorrupted: if the persisted data fails
            integrity checks.
        """

    @abc.abstractmethod
    def save(self, snapshot: SessionSnapshot) -> None:
        """Atomically replace the persisted snapshot."""

    @abc.abstractmethod
    def clear(self) -> None:
        """Drop all persisted state."""

    def close(self) -> None:
        """Release any resources held by the store."""


def _message_to_dict(message: StoredMessage) -> dict[str, Any]:
    return {
        "direction": message.direction,
        "mid": message.mid,
        "topic": base64.b64encode(message.topic).decode("ascii"),
        "payload": base64.b64encode(message.payload).decode("ascii"),
        "qos": message.qos,
        "retain": message.retain,
        "dup": message.dup,
        "state": message.state,
    }


def _message_from_dict(data: Any) -> StoredMessage:
    if not isinstance(data, dict):
        raise SessionStoreCorrupted("message entry is not an object")
    try:
        message = StoredMessage(
            direction=data["direction"],
            mid=int(data["mid"]),
            topic=base64.b64decode(data["topic"], validate=True),
            payload=base64.b64decode(data["payload"], validate=True),
            qos=int(data["qos"]),
            retain=bool(data["retain"]),
            dup=bool(data["dup"]),
            state=int(data["state"]),
        )
    except (KeyError, TypeError, ValueError) as err:
        raise SessionStoreCorrupted(f"invalid message entry: {err}") from err
    if message.direction not in ("in", "out"):
        raise SessionStoreCorrupted(f"invalid direction: {message.direction!r}")
    if not 0 < message.mid < 65536:
        raise SessionStoreCorrupted(f"invalid mid: {message.mid}")
    if message.qos not in (0, 1, 2):
        raise SessionStoreCorrupted(f"invalid qos: {message.qos}")
    return message


class FileSessionStore(SessionStore):
    """Reference `SessionStore` implementation using a single local file.

    Every snapshot is serialized to JSON, appended with a SHA-256
    checksum, written to a temporary file, fsynced and atomically
    renamed over the target file. A crash therefore cannot leave a
    partially written file behind, and any corruption (torn write on
    filesystems without atomic rename, bit rot, tampering) is detected
    on `load` and reported as `SessionStoreCorrupted`.
    """

    def __init__(self, path: "str | os.PathLike[str]") -> None:
        self._path = os.fspath(path)

    @property
    def path(self) -> str:
        """The file this store persists to."""
        return self._path

    def load(self) -> SessionSnapshot:
        try:
            with open(self._path, "rb") as file:
                raw = file.read()
        except FileNotFoundError:
            return SessionSnapshot()
        except OSError as err:
            raise SessionStoreError(f"unable to read session store: {err}") from err

        header_len = len(_MAGIC) + _DIGEST_LEN
        if len(raw) < header_len or not raw.startswith(_MAGIC):
            raise SessionStoreCorrupted(
                f"{self._path}: bad magic or truncated file")
        payload = raw[len(_MAGIC):-_DIGEST_LEN]
        digest = raw[-_DIGEST_LEN:]
        if hashlib.sha256(payload).digest() != digest:
            raise SessionStoreCorrupted(
                f"{self._path}: checksum mismatch, refusing to load")

        try:
            doc = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as err:
            raise SessionStoreCorrupted(
                f"{self._path}: invalid payload: {err}") from err

        if not isinstance(doc, dict) or doc.get("version") != _FORMAT_VERSION:
            raise SessionStoreCorrupted(
                f"{self._path}: unsupported or missing format version")
        try:
            last_mid = int(doc["last_mid"])
            messages = [_message_from_dict(m) for m in doc["messages"]]
        except KeyError as err:
            raise SessionStoreCorrupted(
                f"{self._path}: missing field {err}") from err
        if not 0 <= last_mid < 65536:
            raise SessionStoreCorrupted(f"{self._path}: invalid last_mid")
        return SessionSnapshot(last_mid=last_mid, messages=messages)

    def save(self, snapshot: SessionSnapshot) -> None:
        doc = {
            "version": _FORMAT_VERSION,
            "last_mid": snapshot.last_mid,
            "messages": [_message_to_dict(m) for m in snapshot.messages],
        }
        payload = json.dumps(doc, separators=(",", ":")).encode("utf-8")
        raw = _MAGIC + payload + hashlib.sha256(payload).digest()

        # Unique temporary name: concurrent save() calls from different
        # threads must not clobber each other's temporary file.
        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{os.path.basename(self._path)}.",
            suffix=".tmp",
            dir=os.path.dirname(os.path.abspath(self._path)),
        )
        try:
            with os.fdopen(fd, "wb") as file:
                file.write(raw)
                file.flush()
                os.fsync(file.fileno())
            os.replace(tmp_path, self._path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
        # Best effort directory fsync so the rename itself is durable.
        try:
            dir_fd = os.open(
                os.path.dirname(os.path.abspath(self._path)), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass

    def clear(self) -> None:
        try:
            os.unlink(self._path)
        except FileNotFoundError:
            pass
