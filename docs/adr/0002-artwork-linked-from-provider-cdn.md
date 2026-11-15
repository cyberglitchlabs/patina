---
status: accepted
---

# Link artwork from the provider's CDN; no local image cache yet

XMLTV `<icon>` elements point at the provider's image host, and Jellyfin downloads and caches the pictures. Patina stores no image files. On 2026-11-02 the old host (`zap2it.tmsimg.com`) returned NXDOMAIN while `zpmc.tmsimg.com` resolved and served images, so the Gracenote provider now uses `zpmc.tmsimg.com` and migration 0009 rewrote the stored URLs. Afterwards 15,630 programmes were stored, 11,035 of them with a working artwork URL.

A local cache was deferred. Storing Gracenote's images goes beyond linking to them, which the design spec (decision 14) calls a legal/terms-of-service step the author would take later and deliberately. The data model already has an image record (source URL, content hash, fetch time, local path), so a cache needs no redesign.

## Consequences

Artwork depends on the provider's host staying up and keeping the same name. That already failed once. Because image URLs are persisted, a host change needs a data migration on both backends, not only a code change.

Reasons to build the cache, as suggested rather than decided: another CDN host change or outage, Jellyfin ceasing to cache images, or the owner accepting the terms-of-service step.
