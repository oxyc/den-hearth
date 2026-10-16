# TorBox managed HLS spike — 2026-10-15

This is a sanitized record of live tests against a TorBox Pro account. No API keys, signed URLs,
titles, filenames, torrent IDs, or file IDs are included.

## Result

The basic managed-HLS path works and is simpler than the provisional design assumed:

- `GET /v1/api/stream/createstream` returned `200` in 1.1–2.1 seconds.
- A 48 MiB MKV and a 1 GiB MKV both produced a playable media playlist on the first probe.
- `GET /v1/api/stream/getstreamdata` returned `200`; it was not needed to make the returned HLS URL
  playable.
- Repeating `createstream` for the same file and choices returned the same URL.
- No create/poll/expire session API is justified by these results.

Hearth should call `createstream`, verify that the returned URL is HTTPS on an allowed TorBox CDN
host, make one bounded playlist probe, and return it. A failed probe is `managed_unavailable`, allowing
Den Web to use its existing Remux fallback.

## Observed response shapes

The checked-in JSON files preserve the fields and types the adapter needs while replacing all private
values. Important behavior not apparent from TorBox's OpenAPI document:

| Case | Observed result |
| --- | --- |
| Valid torrent and file | HTTP `200`, `success: true`, `data.hls_url` present |
| Unknown torrent | HTTP `500`, `success: false`, `error: ITEM_NOT_FOUND` |
| Unknown file | HTTP `500`, `success: false`, `error: ITEM_NOT_FOUND` |
| Embedded subtitle selected | create returned success and a distinct URL; that URL returned an unstructured HTTP `500` for more than 30 seconds |

The subtitle result is a provider failure, not a useful pending state: there was no retry hint or
structured progress response. Do not add a general session resource for it.

For v1, leave `chosen_subtitle_index` unset and use the default audio track. Den can keep its existing
external-subtitle path. Multi-audio selection needs its own live fixture before it becomes public API.

## Scrobbling

Every request included `scrobbling_enabled=false`. TorBox echoed `false` in the create response and
put the same setting in the HLS URL. The test account had no linked Trakt, Simkl, AniList, or MAL
integration, so an external scrobble side effect could not be independently observed.

The adapter must always send `false` and reject a success response that does not echo `false`. Cover
that invariant with a fixture test.

## Playlist and browser behavior

- The returned object was a VOD media playlist, not a master playlist.
- It contained 255 relative MPEG-TS segment paths, five-second target duration, and `ENDLIST`.
- No key, alternate-audio, subtitle, or redirect origin appeared in the successful playlist.
- Playlist and segment requests stayed on `https://flux-002.wnam.tb-cdn.io` in this run.
- Playlist and segment responses allowed cross-origin reads with `Access-Control-Allow-Origin: *`.
- First and late segment range requests both returned `206` with `Content-Range`.
- Segment bytes were valid MPEG-TS. TorBox served `.ts` with the incorrect
  `text/vnd.trolltech.linguist` content type, but hls.js still handled it.
- A real headless Chromium + hls.js run parsed the manifest, loaded fragments, played, and sought to
  80% without a fatal hls.js error.

The concrete host is server- and region-shaped. Validate returned hosts as subdomains of
`tb-cdn.io`, and use the narrow CSP source `https://*.tb-cdn.io`; do not pin this one `flux` host.

## URL lifetime and refresh

The HLS URL carried an opaque UUID-like token and no expiry. Neither create response exposed an expiry
time. Repeating create after a short delay returned the same still-working URL. TorBox documents a
three-hour lifetime for download links, but does not document that lifetime for managed HLS, so it
must not be copied into this contract.

Do not return an invented `expiresAt`. If playback later fails because the URL is no longer valid, Den
Web should repeat the same managed-play request once and otherwise fall back to Remux.

## Account capability

The successful live account reported plan code `2` and was shown as Pro in TorBox's account UI. Other
plan levels were not tested. Hearth must attempt the operation and classify the response; it must not
gate this feature by a locally maintained plan-name table.
