"""In-memory MQTT connection for tests in environments without real sockets.

The test sandbox forbids real ``socket()``/``bind()``/``connect()`` and even
real ``send()``/``recv()`` calls, so the fake broker tests cannot use a TCP
listener. ``PipeBroker`` connects to a paho ``Client`` through in-memory byte
buffers; real pipe file descriptors (holding a permanent sentinel byte) are
carried along purely so that ``select.select`` in the client loop has valid
readable/writable descriptors, while all data movement is intercepted by the
fake socket object.
"""
from __future__ import annotations

import os
import select as select_module
import threading
import time
from collections import deque
from typing import Any

import paho.mqtt.client as paho_client

_REAL_SELECT = select_module.select


def install_socketpair_patch() -> None:
    """Replace the client's internal wakeup pipe with in-memory objects.

    The real ``send``/``recv`` syscalls are blocked in the test sandbox, but
    ``select`` still needs valid file descriptors, so a real AF_UNIX pair is
    created purely for its fds while all data is kept in memory.
    """
    paho_client._socketpair_compat = _fake_socketpair


def _fake_socketpair() -> tuple["_WakeSocket", "_WakeSocket"]:
    read_fd, write_fd = os.pipe()
    # A sentinel byte makes the reader fd permanently readable so that the
    # client's select() wakes up immediately instead of waiting for its
    # timeout; in-memory data transfer is handled by the fake objects.
    os.write(write_fd, b"\x00")
    reader = _WakeSocket(read_fd)
    writer = _WakeSocket(write_fd, peer=reader)
    reader.peer = writer
    return reader, writer


class _WakeSocket:
    def __init__(self, fd: int, peer: "_WakeSocket | None" = None) -> None:
        self._fd = fd
        self._signalled = threading.Event()
        self.peer = peer

    def fileno(self) -> int:
        return self._fd

    def setblocking(self, flag: bool) -> None:
        pass

    def send(self, data: bytes) -> int:
        if self.peer is not None:
            self.peer._signalled.set()
        return len(data)

    def recv(self, size: int) -> bytes:
        if self._signalled.is_set():
            self._signalled.clear()
        return b"\x00"

    def close(self) -> None:
        try:
            os.close(self._fd)
        except OSError:
            pass


def _read_exact(stream: "_ByteStream", length: int) -> bytes:
    data = bytearray()
    while len(data) < length:
        chunk = stream.read(1, timeout=10)
        if not chunk:
            raise ConnectionError("fake broker: connection closed by client")
        data += chunk
    return bytes(data)


def _decode_remaining_length(stream: "_ByteStream") -> int:
    multiplier = 1
    value = 0
    for _ in range(4):
        (byte,) = _read_exact(stream, 1)
        value += (byte & 127) * multiplier
        if byte & 128 == 0:
            return value
        multiplier *= 128
    raise ValueError("malformed remaining length")


def read_packet(stream: "_ByteStream") -> bytes | None:
    """Read one complete MQTT packet from ``stream``; None on closed pipe."""
    first = stream.read(1, timeout=10)
    if not first:
        return None
    remaining = _decode_remaining_length(stream)
    return first[0:1] + _encoded_remaining_length(remaining) + _read_exact(
        stream, remaining
    )


def _encoded_remaining_length(length: int) -> bytes:
    encoded = bytearray()
    while True:
        byte = length % 128
        length //= 128
        if length > 0:
            byte |= 128
        encoded.append(byte)
        if length == 0:
            return bytes(encoded)


class _ByteStream:
    """One direction of an in-memory ordered byte stream."""

    def __init__(self) -> None:
        self._chunks: deque[bytes] = deque()
        self._cond = threading.Condition()
        self.closed = False

    def write(self, data: bytes) -> None:
        with self._cond:
            if self.closed:
                raise BrokenPipeError("stream closed")
            self._chunks.append(bytes(data))
            self._cond.notify_all()

    def read(self, size: int, timeout: float | None = None) -> bytes:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while not self._chunks:
                if self.closed:
                    return b""
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return b""
                self._cond.wait(remaining)
            data = b"".join(self._chunks)
            self._chunks.clear()
            if len(data) > size:
                self._chunks.append(data[size:])
                data = data[:size]
            return data

    def drain(self) -> bytes:
        with self._cond:
            data = b"".join(self._chunks)
            self._chunks.clear()
            return data

    def close(self) -> None:
        with self._cond:
            self.closed = True
            self._cond.notify_all()


class FakeSocket:
    """Socket-like object backed by two :class:`_ByteStream` instances.

    ``read_fd`` holds an unread sentinel byte (always readable to select);
    ``write_fd`` is the write end of another pipe (always writable). Real
    data never crosses these fds.
    """

    def __init__(self, read_fd: int, write_fd: int, to_client: _ByteStream,
                 to_broker: _ByteStream) -> None:
        self._read_fd = read_fd
        self._write_fd = write_fd
        self._to_client = to_client
        self._to_broker = to_broker
        self._blocking = True

    def fileno(self) -> int:
        return self._read_fd

    def select_fileno(self, for_write: bool) -> int:
        return self._write_fd if for_write else self._read_fd

    def setblocking(self, flag: bool) -> None:
        self._blocking = bool(flag)

    def settimeout(self, value: float | None) -> None:
        pass

    def gettimeout(self) -> float | None:
        return None

    def pending(self) -> int:
        return 0

    def recv(self, size: int) -> bytes:
        with self._to_client._cond:
            if not self._to_client._chunks:
                if not self._blocking:
                    raise BlockingIOError()
                return b""
        return self._to_client.read(size, timeout=None)

    def send(self, data: bytes) -> int:
        self._to_broker.write(data)
        return len(data)

    def sendall(self, data: bytes) -> None:
        self._to_broker.write(data)

    def close(self) -> None:
        self._to_broker.close()

    def setsockopt(self, *args: Any) -> None:
        pass

    def shutdown(self, *args: Any) -> None:
        pass


def _patched_select(rlist, wlist, xlist, timeout=None):
    """select() shim mapping FakeSocket objects to their read/write fds."""
    def map_fd(obj, for_write):
        if isinstance(obj, FakeSocket):
            return obj.select_fileno(for_write)
        return obj

    r_mapped = [map_fd(obj, False) for obj in rlist]
    w_mapped = [map_fd(obj, True) for obj in wlist]
    r_ready, w_ready, x_ready = _REAL_SELECT(
        r_mapped, w_mapped, xlist,
        timeout if timeout is not None else None,
    )
    # Map numeric fds back to the corresponding fake objects where possible.
    r_objects = [
        obj for obj in rlist
        if (obj.select_fileno(False) if isinstance(obj, FakeSocket)
            else getattr(obj, "fileno", lambda: obj)()) in r_ready
    ]
    w_objects = [
        obj for obj in wlist
        if (obj.select_fileno(True) if isinstance(obj, FakeSocket)
            else getattr(obj, "fileno", lambda: obj)()) in w_ready
    ]
    return r_objects, w_objects, x_ready


class PipeBroker:
    """A broker-side end of an in-memory connection.

    One :class:`PipeBroker` accepts several successive connections (used to
    simulate disconnect/reconnect cycles), each handing out fresh streams
    through :meth:`next_connection`.
    """

    def __init__(self) -> None:
        self._pending: deque[
            tuple[_ByteStream, _ByteStream, int, int]
        ] = deque()
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._extra_fds: list[int] = []
        self._client: Any = None

    def attach_client(self, client: Any) -> None:
        """Register the paho client and patch its socket/wakeup plumbing."""
        self._client = client
        client._create_socket = self.make_client_socket
        paho_client.select.select = _patched_select

    def make_client_socket(self) -> FakeSocket:
        to_client = _ByteStream()
        to_broker = _ByteStream()
        # One read end with an unread byte is permanently readable and one
        # write end is permanently writable, so select() in the client loop
        # always reports the fake socket ready; bytes flow in memory.
        read_fd, read_probe = os.pipe()
        os.write(read_probe, b"\x00")
        write_probe, write_fd = os.pipe()
        self._extra_fds.extend((read_fd, read_probe, write_probe, write_fd))
        with self._cond:
            self._pending.append(
                (to_client, to_broker, read_fd, write_fd)
            )
            self._cond.notify_all()
        return FakeSocket(read_fd, write_fd, to_client, to_broker)

    def next_connection(self, timeout: float = 10.0
                        ) -> tuple[_ByteStream, _ByteStream]:
        with self._cond:
            deadline = time.monotonic() + timeout
            while not self._pending:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("fake broker: client did not reconnect")
                self._cond.wait(remaining)
            to_client, to_broker, _fd, _write_fd = self._pending.popleft()
        return to_broker, to_client

    @staticmethod
    def drop(connection: tuple[_ByteStream, _ByteStream]) -> None:
        """Simulate the broker abruptly closing ``connection``."""
        broker_in, to_client = connection
        broker_in.close()
        to_client.close()

    def close(self) -> None:
        paho_client.select.select = _REAL_SELECT
        for fd in getattr(self, "_extra_fds", []):
            try:
                os.close(fd)
            except OSError:
                pass

    def wake_client(self) -> None:
        """Interrupt the client's select() so it notices injected bytes.

        The injected bytes live in memory and cannot make a file descriptor
        readable, so poke the client's own wakeup socketpair instead.
        """
        if self._client is not None and self._client._sockpairW is not None:
            try:
                self._client._sockpairW.send(b"\x00")
            except (OSError, AttributeError):
                pass

    def send(self, to_client: _ByteStream, packet: bytes) -> None:
        to_client.write(packet)
        self.wake_client()
