# Apple TV wake controller

This optional Hearth-owned sidecar uses pyatv **0.18.0** and the Companion protocol only. It receives bounded,
body-free wake events from Edge, wakes the paired Apple TV and launches the exact installed Den bundle selected
during setup. The TV still drains, authenticates and freshness-checks the sealed play.

The controller image is separate from the Rust media image. Run it with host networking: multicast discovery and
Companion traffic cannot be assumed to cross Hearth's ordinary container bridge. It publishes no TCP port. Its
only local API is a mode-0600 Unix socket shared with Hearth.

## Setup

Create a root-owned bind directory writable as uid 65532, or use the Compose volume. Enroll the controller in Edge
from a library-member UI, then import the one-time `<32hex>.<43 base64url>` credential without putting it in shell
history:

```sh
docker compose --env-file selfhost/.env -f selfhost/compose.yaml --profile apple-tv run --rm \
  apple-tv-controller enroll
```

Discover and pair (the Apple TV displays the PIN):

```sh
docker compose --env-file selfhost/.env -f selfhost/compose.yaml --profile apple-tv run --rm \
  apple-tv-controller discover
docker compose --env-file selfhost/.env -f selfhost/compose.yaml --profile apple-tv run --rm \
  apple-tv-controller pair --id <pyatv-id>
docker compose --env-file selfhost/.env -f selfhost/compose.yaml --profile apple-tv run --rm \
  apple-tv-controller apps --id <pyatv-id>
```

The authenticated Den install flow registers `{v, deviceId, name, householdInbox, assistantLane, bundleId}` through
Hearth's configured-install route. For recovery or diagnostics, an operator can send that exact JSON object on
standard input to `docker compose exec -T apple-tv-controller den-hearth-controller register`. The permissioned
socket framing is one JSON line:

```json
{"op":"upsert","target":{"v":1,"deviceId":"0011223344556677","name":"Living Room","householdInbox":"<64 hex>","assistantLane":"<32 hex>","bundleId":"fi.oxy.den.TEAM"}}
```

Hearth accepts authenticated `PUT /den/<config>/wake/target`, validates the exact object, and forwards it to
`/run/den-controller/controller.sock`. The Compose services share only that uid-65532 runtime socket volume. They do
not share controller state or pyatv credentials, and there is no browser-to-localhost API. The `register` CLI remains
available for recovery and diagnostics.

After registration, map that logical TV to the paired physical receiver. `target` fetches the installed app list and
refuses a bundle that is absent, so a re-signed Den installation is never guessed:

```sh
docker compose --env-file selfhost/.env -f selfhost/compose.yaml --profile apple-tv run --rm \
  apple-tv-controller target --device-id 0011223344556677 --apple-tv-id <pyatv-id> \
  --bundle-id fi.oxy.den.TEAM
docker compose --env-file selfhost/.env -f selfhost/compose.yaml --profile apple-tv up -d
```

The bundle reported by later TV registrations is only a setup suggestion. It never replaces the operator-selected
bundle; run `target` again after a legitimate re-sign. A missing selected bundle produces `app-missing` and no other
app is launched.

Back up the controller state volume. `controller.json` and `pyatv.json` are mode 0600 and written in a mode-0700
directory; state replacement is atomic and fsynced. Losing it requires pairing and enrollment again.

## Runtime contract

The sidecar continuously sends the complete target snapshot to `POST /wake/poll?wait=25` with its
`x-den-wake-controller` credential. Requests and responses are bounded. Events are deduplicated durably, concurrent
events for one target share one attempt, and a 30-second cooldown prevents repeated launch. Identifier discovery is
tried first; the last address is only a unicast fallback. Reconnect uses capped exponential backoff with full jitter.

Logs contain only target/event ids, stages, bounded reasons and timings. They never include controller or Apple TV
credentials, queue ids, tokens, bundle ids, sealed messages, titles or URLs. `launch-requested` and `receiver-ready`
mean only that the Companion operation reached those stages; neither claims Den opened or playback began.

`den-hearth-controller health` prints redacted process, Edge connection, configured-target and mapped-target state.
Real hardware qualification still must record Apple TV model, tvOS, CEC chain, network and pyatv version and prove
the full queue-to-progress path remains comfortably below 120 seconds.
