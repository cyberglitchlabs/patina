---
status: accepted
---

# Emit a bare numeric display-name for every channel

Each XMLTV channel carries two display-names: `<number> <name>` for people, then the bare `<number>` for Jellyfin. Jellyfin learns a channel's number only from a purely numeric display-name, and it matches feed channels to tuner channels by channel id, then tuner channel id, then number, then normalized name. Before commit 95259cb the feed parsed and held about 15.6k programmes, yet none attached to a tuner channel, so the guide was blank.

## Considered options

- **Change the channel id.** Rejected: ids come from one rule (`canonical_id`) and are shared with the master guide, hindsight accuracy rows and overrides, so a change touches persisted data.
- **Emit only the bare number.** Rejected: it loses the readable name.

## Consequences

Another XMLTV client may show the second display-name as an extra alias. None has been seen; Jellyfin is the only consumer today. `tests/xmltv.rs` and `tests/golden/fixture_slots.xml` pin the behaviour.

The matching order was read in Jellyfin's source (the clouddrop session confirmed the number path by running Jellyfin's own XMLTV reader against Patina's feed). Why name matching fails for `<number> <name>` was not traced line by line. The Jellyfin version is the configured tag (12.2), not a build string read from a running pod.
