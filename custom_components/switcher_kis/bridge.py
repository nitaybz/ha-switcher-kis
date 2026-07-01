# Copyright Tomer Figenblat.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Hardened UDP bridge for the Switcher integration.

This is a drop-in replacement for ``aioswitcher.bridge.SwitcherBridge`` that keeps
the upstream packet parser (imported below, so it stays in sync with the pinned
library) but replaces the socket layer, which is the source of the "devices go
unavailable until a full Home Assistant restart" behaviour.

Fixes over the upstream bridge:

* Sockets are opened with ``SO_REUSEADDR`` (and ``SO_REUSEPORT`` where available)
  and ``SO_BROADCAST`` so a port that has not been fully released yet can be
  rebound, and so a second short-lived listener (config-flow discovery) can run
  alongside the entry's listener without an ``address already in use`` error.
* A partial bind failure no longer leaks the ports that were already bound. If
  any port fails to bind, every port opened so far is closed before the error is
  raised, so the process is never left holding orphaned sockets.
* ``stop()`` waits for each transport to actually close before returning and
  clears the transport map, so an immediate rebind does not race the deferred
  close of the previous socket.
* A malformed or unknown-model datagram can no longer raise into the event loop.
  The parser call is guarded, so one bad packet is dropped instead of killing the
  reception of every other device.
* A socket that drops unexpectedly (``connection_lost`` outside of ``stop()``) is
  rebound automatically instead of staying dead until the integration is
  reloaded.
"""

from __future__ import annotations

import asyncio
import socket
from functools import partial
from logging import getLogger
from typing import Any, Callable

from aioswitcher.bridge import (
    SWITCHER_UDP_BROADCAST_PORTS,
    _parse_device_from_datagram,
)
from aioswitcher.device import SwitcherBase

__all__ = ["SwitcherBridge"]

logger = getLogger(__name__)

# How long stop() waits for a transport to confirm it has closed.
_CLOSE_TIMEOUT_SEC = 2.0
# Delay before an unexpectedly dropped socket is rebound.
_REBIND_DELAY_SEC = 1.0


def _create_broadcast_socket(port: int) -> socket.socket:
    """Create a non-blocking UDP socket bound to a broadcast port.

    The reuse options are what allow a rebind (on reload) or a concurrent
    discovery listener to bind a port that is still, or already, in use.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except OSError:
            # Not supported on every platform, reuse-addr is enough on its own.
            pass
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.setblocking(False)
    sock.bind(("", port))
    return sock


class SwitcherBridge:
    """UDP client that bridges Switcher device broadcast messages.

    Args:
        on_device: a callable to which every SwitcherBase device found is sent.

    """

    def __init__(self, on_device: Callable[[SwitcherBase], Any]) -> None:
        """Initialize the switcher bridge."""
        self._on_device = on_device
        self._is_running = False
        self._stopping = False
        self._transports: dict[int, asyncio.DatagramTransport] = {}
        self._protocols: dict[int, _UdpClientProtocol] = {}

    async def __aenter__(self) -> SwitcherBridge:
        """Enter SwitcherBridge asynchronous context manager."""
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        """Exit the SwitcherBridge asynchronous context manager."""
        await self.stop()

    async def start(self) -> None:
        """Create the asynchronous listeners and start the bridge."""
        self._stopping = False
        loop = asyncio.get_running_loop()
        try:
            for port in SWITCHER_UDP_BROADCAST_PORTS:
                logger.info("starting the udp bridge on port %s", port)
                await self._bind_port(loop, port)
        except OSError:
            # Do not leak the ports that were already bound before the failure.
            logger.error("failed to start udp bridge, releasing bound ports")
            await self.stop()
            raise
        self._is_running = True

    async def _bind_port(self, loop: asyncio.AbstractEventLoop, port: int) -> None:
        """Bind a single broadcast port and register its protocol."""
        sock = _create_broadcast_socket(port)
        protocol = _UdpClientProtocol(
            partial(_parse_device_from_datagram, self._on_device),
            partial(self._on_connection_lost, port),
        )
        transport, _ = await loop.create_datagram_endpoint(
            lambda: protocol, sock=sock
        )
        self._transports[port] = transport
        self._protocols[port] = protocol
        logger.debug("udp bridge on port %s started", port)

    async def stop(self) -> None:
        """Stop the bridge and wait for every socket to be released."""
        self._stopping = True
        closes = []
        for port in list(self._transports):
            transport = self._transports.get(port)
            protocol = self._protocols.get(port)
            if transport is not None and not transport.is_closing():
                logger.info("stopping the udp bridge on port %s", port)
                transport.close()
            if protocol is not None:
                closes.append(protocol.wait_closed())
        # Wait for the OS to actually release the file descriptors so that an
        # immediate rebind does not hit "address already in use".
        if closes:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*closes), timeout=_CLOSE_TIMEOUT_SEC
                )
            except TimeoutError:
                logger.warning("timed out waiting for udp bridge to close")
        self._transports.clear()
        self._protocols.clear()
        self._is_running = False

    async def restart(self) -> None:
        """Fully tear down and rebuild the listeners in place.

        Used by the integration watchdog to recover a wedged listener without a
        reload or a Home Assistant restart.
        """
        logger.info("restarting the switcher udp bridge")
        await self.stop()
        await self.start()

    def _on_connection_lost(self, port: int, exc: Exception | None) -> None:
        """Rebind a port whose socket dropped unexpectedly."""
        if self._stopping:
            return
        logger.warning(
            "udp bridge lost the socket on port %s (%s), rebinding", port, exc
        )
        self._transports.pop(port, None)
        self._protocols.pop(port, None)
        asyncio.get_running_loop().call_later(
            _REBIND_DELAY_SEC, lambda: asyncio.ensure_future(self._rebind(port))
        )

    async def _rebind(self, port: int) -> None:
        """Rebind a single dropped port, retrying on transient failure."""
        if self._stopping:
            return
        try:
            await self._bind_port(asyncio.get_running_loop(), port)
            logger.info("udp bridge rebound port %s", port)
        except OSError as err:
            logger.warning("failed to rebind udp port %s (%s), retrying", port, err)
            asyncio.get_running_loop().call_later(
                _REBIND_DELAY_SEC, lambda: asyncio.ensure_future(self._rebind(port))
            )

    @property
    def is_running(self) -> bool:
        """Return true if the bridge is running."""
        return self._is_running


class _UdpClientProtocol(asyncio.DatagramProtocol):
    """Asyncio UDP protocol that guards parsing and reports connection loss."""

    def __init__(
        self,
        on_datagram: Callable[[bytes], None],
        on_lost: Callable[[Exception | None], None],
    ) -> None:
        """Initialize the protocol."""
        self.transport: asyncio.BaseTransport | None = None
        self._on_datagram = on_datagram
        self._on_lost = on_lost
        self._closed: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Call on connection established."""
        self.transport = transport

    def datagram_received(self, data: bytes, addr: tuple[Any, ...]) -> None:
        """Call on datagram received.

        Guarded so a malformed or unknown-model packet is dropped instead of
        raising into the event loop and interrupting reception for other devices.
        """
        try:
            self._on_datagram(data)
        except Exception:  # noqa: BLE001 - one bad packet must not stop the bridge
            logger.debug("dropping an unparseable switcher datagram", exc_info=True)

    def error_received(self, exc: Exception | None) -> None:
        """Call on a datagram error, non fatal for the socket."""
        logger.debug("udp client received an error: %s", exc)

    def connection_lost(self, exc: Exception | None) -> None:
        """Call on connection lost."""
        if not self._closed.done():
            self._closed.set_result(None)
        self._on_lost(exc)

    async def wait_closed(self) -> None:
        """Wait until the transport has fully closed."""
        await self._closed
