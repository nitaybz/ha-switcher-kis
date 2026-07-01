# Switcher for Home Assistant (resilient fork)

A drop-in replacement for the built-in [Switcher](https://www.home-assistant.io/integrations/switcher_kis/)
(`switcher_kis`) integration that fixes the long-standing problem of Switcher
devices going **unavailable in Home Assistant while they still work fine in the
Switcher app**, and the reload loop where the integration cycles between an error
state and an OK state until a full Home Assistant restart.

It keeps the same domain (`switcher_kis`), the same configuration, and the same
entities, so installing it simply overrides the built-in integration. Nothing to
reconfigure.

## Why devices go unavailable on the stock integration

The stock integration is push only. It never asks a device anything, it just
listens for the UDP status broadcast that every Switcher device sends every few
seconds, and marks a device unavailable if no broadcast arrives within 30
seconds. The Switcher app talks to the device over a separate path (cloud and
direct TCP), so the app keeps working even when those broadcasts stop reaching
Home Assistant.

On top of that, the underlying UDP listener is fragile:

- the listener sockets are opened without address reuse, so a reload can fail to
  rebind a port that has not been fully released yet
- a partial bind failure leaks the ports that were already bound, with no way to
  release them short of restarting Home Assistant
- a malformed or unknown-model packet can raise straight into the event loop and
  interrupt reception for every other device
- a socket that drops is never re-established

Those are exactly the conditions that produce "works in the app, unavailable in
Home Assistant, only a restart fixes it".

## What this fork changes

**Resilient UDP listener** (`bridge.py`, replaces the library socket layer while
reusing its packet parser):

- sockets use `SO_REUSEADDR` (and `SO_REUSEPORT` where available) plus
  `SO_BROADCAST`, so a reload can rebind immediately and the config-flow
  discovery listener can run alongside the entry listener without a port clash
- a partial bind failure releases every port it already opened instead of
  leaking them
- `stop()` waits for each socket to actually close before returning, so a rebind
  never races the previous socket
- one malformed or unknown-model packet is dropped, it can no longer stop
  reception for the other devices
- a socket that drops unexpectedly is rebound automatically

**Poll on silence** (`coordinator.py`): when a device misses its broadcasts, the
integration probes it over TCP before declaring it unavailable. If it answers it
stays available and the next broadcast refreshes the live state. If it does not
answer it is marked unavailable while the listener keeps waiting for it to come
back on its own. No reload, no restart.

**Listener watchdog** (`__init__.py`): if no device has broadcast for two minutes
while devices are known, the listener is assumed wedged and rebuilt in place.
Recovery does not require a reload or a Home Assistant restart.

**Immediate IP-change handling**: device state is keyed by device id, so a device
that changes IP (DHCP) is picked up from its next broadcast and all control and
polling follow the new address right away.

## Installation

### HACS (recommended)

1. HACS, three-dot menu, Custom repositories.
2. Add `https://github.com/nitaybz/ha-switcher-kis` as an Integration.
3. Install "Switcher (resilient)".
4. Restart Home Assistant.

Because it shares the `switcher_kis` domain, it takes over from the built-in
integration automatically. Your existing Switcher config entry keeps working.

### Manual

Copy `custom_components/switcher_kis` into your Home Assistant `config/custom_components`
folder and restart.

## Network requirements (unchanged from the stock integration)

The device broadcasts still have to reach Home Assistant for fast state updates:

- Home Assistant and the Switcher devices should be on the same subnet, no client
  or AP isolation between them
- open incoming UDP `20002`, `20003`, `10002`, `10003` and outgoing TCP `9957`,
  `10000` on the host
- in Docker, use host networking so broadcasts are not dropped by the container
  network namespace

With this fork, if the broadcasts are only intermittent the TCP poll keeps
reachable devices available instead of flapping them.

## Newer token-gated models

Runner S11/S12, the SL lights and the Heater need the Switcher account token to be
controlled and polled. Add it in the integration options (obtained from Switcher's
GetKey service, keyed by your account email). Older plugs, water heaters, Breeze
and the classic Runner do not need a token.

## Relationship to upstream

The fixes here are being submitted upstream to
[home-assistant/core](https://github.com/home-assistant/core) and
[aioswitcher](https://github.com/TomerFi/aioswitcher). This repository exists so
the fix is usable today and until those land.

## License

Apache 2.0. Derived from the Home Assistant `switcher_kis` integration and the
`aioswitcher` library, both Apache 2.0.
