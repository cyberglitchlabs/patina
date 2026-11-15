# Patina

Patina builds one well-groomed TV guide from several unreliable guide providers and serves it to media servers such as Jellyfin. This glossary fixes the words used in the code, the docs and conversations between people and agents.

## Language

### Guide data

**Master guide**:
The persisted, merged guide Patina holds. Every XMLTV feed is rendered from it, and a bad fetch never makes it worse.
_Avoid_: Database, EPG, schedule

**Slot**:
One programme airing on one channel in the master guide, after providers were merged.
_Avoid_: Programme, listing, event

**Observation**:
One provider's claim about one programme, with that provider's own times and the moment it was fetched.
_Avoid_: Record, entry, result

**Provider**:
An external source of guide data, such as Gracenote.
_Avoid_: Source (that word means a channel source), vendor, upstream

**Canonical channel**:
A channel as the media server sees it, with a stable id derived from the call sign by one rule (`canonical_id`).
_Avoid_: Station, channel record

### Where channels come from

**Source**:
A configured list of tuner channels, such as an HDHomeRun lineup URL or a static list. Each source gets its own XMLTV feed.
_Avoid_: Provider, tuner config

**Tuner channel**:
A channel a source lists. It is matched to a provider channel by channel number first, then by call sign, unless an override says otherwise.
_Avoid_: Lineup entry

**Override**:
A manual instruction that maps one tuner channel to a canonical channel (or excludes it), for cases automatic matching gets wrong.
_Avoid_: Mapping, exception

**Feed**:
One XMLTV document Patina serves, rendered from the master guide: one per source, plus a combined one.
_Avoid_: Output, export, EPG file

**Lineup**:
A provider-side channel set requested together, identified by a name in the configuration. One provider can have several.
_Avoid_: Region, market

### Fetching

**Window**:
A fixed six-hour span of time that a provider is asked for in one request. The scheduler works through windows nearest-first.
_Avoid_: Slice, chunk, page

**Tick**:
One pass of the scheduler over every window that is due.
_Avoid_: Cycle, run, sweep

**Daily budget**:
The hard cap on requests Patina may send one provider per UTC day. It survives restarts.
_Avoid_: Rate limit, quota (the rate limit is the spacing between requests)

**Gate**:
A check the fetch layer makes before sending a request (budget, circuit breaker, known-bad window, rate limit). If a gate cannot be checked, the request is not sent.
_Avoid_: Guard, filter, throttle

### Merging and scoring

**Arbitration**:
Choosing, per slot, which provider's observation wins. Fields the winner lacks are filled from lower-ranked observations.
_Avoid_: Reconciliation, conflict resolution

**Hindsight accuracy**:
How closely a provider's earlier listing of a slot matched its later listing for the same slot, kept as a running average per provider and channel.
_Avoid_: Accuracy history, drift

**Window ingest**:
Taking one provider's parsed window and putting it into the master guide: storing its channels, checking its timeline, merging each channel, and recording hindsight accuracy.
_Avoid_: Import, sync, store step
