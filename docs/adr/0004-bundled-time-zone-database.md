---
status: accepted
---

# Time-zone rules are compiled into the binary

Patina renders XMLTV times with the UTC offset that applies at each instant in the configured zone. `jiff` supplies the rules, with its tz database bundled in (`tzdb-bundle-always`) and the host's zoneinfo never consulted.

The runtime image is `distroless/cc`, which has no `/usr/share/zoneinfo`, so system data would make every zone lookup fail there. The same choice also gives identical behavior on every host.

The cost: a government changing a daylight-saving rule reaches Patina only with a release built on a newer `jiff-tzdb`. Updating tzdata on the host does nothing.

## Consequences

Keep `tzdb-bundle-always` while the image has no zoneinfo. Zone names are matched exactly (`America/Chicago`, not `america/chicago`).
