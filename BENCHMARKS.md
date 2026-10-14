# Benchmarks

Every optimisation in [oxyc/den#282](https://github.com/oxyc/den/issues/282) is judged against what is measured here:
measure first, adopt complexity only for about 2x on the target workload. This page says how to re-run each harness and
holds the baseline tables. The raw results are JSONL under `bench/results/`, one object per benchmark or case.

| Harness | What it answers | Run |
|---|---|---|
| Microbenchmarks (divan, with allocation counts) | CPU and allocations per operation of the hot paths in scout, subtitles, remux and reel | `bench/micro.sh` |
| Rust vs Go | the same inputs through den-scout's Go code | `bench/go/run.sh` |
| HTTP load and soak, **enforcing** | req/s, latency, memory, descriptors and sockets of the release binary in a memory-capped container; fails on OOM, a hit limit, leaks or a missing health recovery | `bench/http/run.py` |
| Native HTTP | the same cases without a container or emulation, for the CPU path | `bench/http/native.py` |
| Test-suite timing | clean and incremental build, per-crate test time, slowest tests | `bench/suite-timing.sh` |

Everything is stdlib Python, shell and the Rust/Go benchmarks themselves. Nothing reads a credential, and no case can
reach a real service (see [The HTTP harness](#the-http-harness)).

## Baseline

**Machine.** Apple M4 (10 CPUs, 24 GiB) running macOS, shared with other work: the load average at the start of each
run is in its `meta` row (roughly 5 to 17 for the runs below, against 10 CPUs). Docker is OrbStack's Linux VM (10 CPUs, 4 GiB, cgroup v2) which
also hosts other containers. **Build.** `cargo bench` uses the workspace's release profile (`opt-level 3`, fat LTO, one
codegen unit); the HTTP run uses the `Dockerfile`'s `build` stage, so the same binary the image ships plus
`--features stub-upstreams` (below). **Date** 2026-10-14. **Commit** `1e507b4` (main) plus this PR; everything the PR
adds sits behind the `bench` and `stub-upstreams` features, so the measured code is main's.

On a laptop with this much else running, **trust the `fastest` column and allocation counts over the medians**; the
medians of the larger rank benchmarks move by 2x between runs. Allocation counts do not depend on the machine.

### Rust vs Go (den-scout)

Go is `origin/main` of den-scout (`07bb96c`, 0.36.1, Go 1.26.8) in a throwaway `git worktree`, with den-scout's own
`BenchmarkPickEpisode_*` and `BenchmarkTieredCache_500DiskMisses` plus `bench/go/hearth_bench_test.go`, five runs each.
Both sides read the inputs `bench/gen_fixtures.py` writes (a 2,000-release list, a Stremio body of 500 streams, a 10 MiB
TorBox listing, an install config) and `crates/scout/testdata`. Go's allocs/op come from `-benchmem`; Rust's from divan's
allocator, which counts the same thing (calls to the allocator per iteration).

| benchmark | Rust fastest | Go fastest | Go / Rust | Rust median | Go median | Go / Rust | Rust allocs/op | Go allocs/op | Rust B/op | Go B/op |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| decode_config_segment/plain | 3.53 µs | 2.24 µs | 0.6x | 3.68 µs | 2.27 µs | 0.6x | 70 | 34 | 7.05 KB | 1.79 KB |
| decode_config_segment/sealed | 26.3 µs | 47.5 µs | 1.8x | 27.6 µs | 48 µs | 1.7x | 71 | 44 | 7.56 KB | 3.03 KB |
| decode_torbox_listing (10.5 MB) | 7.68 ms | 27.2 ms | 3.5x | 7.80 ms | 27.6 ms | 3.5x | 4,766 | 9,584 | 763 KB | 11.5 MB |
| memory_get_hit | 132 ns | 39.4 ns | 0.3x | 140 ns | 39.8 ns | 0.3x | 2.19 | 0 | 1.14 KB | 0 B |
| memory_get_miss | 18.1 ns | 4.88 ns | 0.3x | 18.2 ns | 4.93 ns | 0.3x | 0 | 0 | 0 | 0 B |
| memory_put_evicting | 120 ns | 102 ns | 0.9x | 199 ns | 105 ns | 0.5x | 3.17 | 2 | 1.18 KB | 112 B |
| parse_stremio_streams/50 | 57.3 µs | 242 µs | 4.2x | 58.2 µs | 247 µs | 4.2x | 1,283 | 687 | 125 KB | 57.1 KB |
| parse_stremio_streams/500 | 587 µs | 2.6 ms | 4.4x | 594 µs | 2.63 ms | 4.4x | 12,756 | 7,088 | 1.23 MB | 568 KB |
| pick_episode_anime24 | 273 µs | 143 µs | 0.5x | 276 µs | 144 µs | 0.5x | 465 | 584 | 51.5 KB | 62.4 KB |
| pick_episode_scene24 | 262 µs | 95.1 µs | 0.4x | 267 µs | 100 µs | 0.4x | 143 | 540 | 4.61 KB | 62.5 KB |
| pick_episode_scene200 | 2.29 ms | 814 µs | 0.4x | 2.33 ms | 819 µs | 0.4x | 1,199 | 4,592 | 39.1 KB | 539 KB |
| pick_episode_whole_season | 6.27 ms | 2.29 ms | 0.4x | 6.42 ms | 2.31 ms | 0.4x | 3,432 | 12,964 | 111 KB | 1.51 MB |
| probe_avi | 2.27 µs | 2.69 µs | 1.2x | 2.36 µs | 2.76 µs | 1.2x | 6 | 3 | 27 B | 16 B |
| probe_matroska/tracks.mkv | 514 ns | 707 ns | 1.4x | 524 ns | 718 ns | 1.4x | 12 | 3 | 143 B | 22 B |
| probe_mp4/sample.mp4 | 1.16 µs | 2.94 µs | 2.5x | 1.18 µs | 3 µs | 2.6x | 38 | 183 | 4.38 KB | 9.34 KB |
| probe_mp4/multi-audio.mp4 | 1.8 µs | 4.29 µs | 2.4x | 1.89 µs | 4.42 µs | 2.3x | 57 | 256 | 6.1 KB | 13.2 KB |
| rank_movie/100 | 690 µs | 1.32 ms | 1.9x | 714 µs | 1.35 ms | 1.9x | 991 | 1,495 | 39.5 KB | 106 KB |
| rank_movie/500 | 3.52 ms | 6.63 ms | 1.9x | 6.12 ms | 10.4 ms | 1.7x | 4,905 | 7,437 | 191 KB | 528 KB |
| rank_movie/2000 | 14.3 ms | 26.2 ms | 1.8x | 29.1 ms | 26.4 ms | 0.9x | 19,330 | 29,945 | 693 KB | 2.1 MB |
| rank_episode/100 | 703 µs | 2.69 ms | 3.8x | 711 µs | 3.84 ms | 5.4x | 792 | 1,257 | 57.1 KB | 124 KB |
| rank_episode/500 | 3.66 ms | 12.6 ms | 3.4x | 4.94 ms | 13 ms | 2.6x | 2,760 | 6,441 | 123 KB | 569 KB |
| rank_episode/2000 | 16.9 ms | 50.6 ms | 3.0x | 31.4 ms | 52.3 ms | 1.7x | 10,294 | 26,465 | 362 KB | 2.71 MB |
| ticket_mint | 3.99 µs | 1.64 µs | 0.4x | 4.07 µs | 1.65 µs | 0.4x | 13 | 8 | 1.3 KB | 2.18 KB |
| ticket_open | 6.96 µs | 2.15 µs | 0.3x | 7.04 µs | 2.18 µs | 0.3x | 43 | 15 | 2.88 KB | 1.59 KB |
| tiered_500_disk_misses | 578 µs | 630 µs | 1.1x | 1.14 ms | 657 µs | 0.6x | 1,500 | 3,500 | 72.5 KB | 240 KB |
| tiered_get_disk_hit | 13.5 µs | 12.1 µs | 0.9x | 15.8 µs | 14 µs | 0.9x | 8 | 13 | 12.5 KB | 9.84 KB |
| tiered_put_durable | 112 µs | 144 µs | 1.3x | 159 µs | 148 µs | 0.9x | 10.5 | 23 | 5.09 KB | 6.46 KB |

(`probe_matroska/hi10p.mkv` and the other two mp4 files are in the raw results.) Reading it:

- **Faster than Go**: the Stremio body parse (4.4x), ranking (1.8 to 3.8x, with 25 to 60% fewer allocations), the MP4
  probe (2.5x, 5x fewer allocations) and the sealed config (1.8x; the X25519 box dominates).
- **Slower than Go, and worth a look**: `ticket_open`/`ticket_mint` (3x and 2.5x slower), episode matching
  (`pick_episode_*`, 2.3 to 2.7x slower), a plain config segment (1.6x slower, twice the allocations) and the in-memory
  cache (`get` is 3x slower: it clones the value, so 2.2 allocations and a copy of the entry per hit, where Go returns the
  string). The causes below are read from the code and not profiled: a ticket's payload goes through a
  `serde_json::Value` and `fold_json_keys` before it is a struct and the AEAD key is expanded on every call.
- **Allocation outlier, fixed by perf/hot-listing**: decoding a 10.5 MB TorBox listing (4,752 torrents) made **457,788
  allocations**, 48x Go's 9,584, at 1.4x Go's speed: about 96 per torrent, from reading every key into a `String`
  (and a second one to lower-case it), every skipped number into a `String`, and a `Vec` per skipped container. It now
  makes 4,766 (the hash strings the map keeps, plus the map's growth) and runs in 7.68 ms against 19.6 ms back to back
  (the before and after runs are in `bench/results/hot-listing-before-*` and `hot-listing-after-*`).

### Hot paths of the other three services (allocations per call)

No Go counterpart exists for these; they are the baseline the later PRs move. Full table in the raw results and below.

| benchmark | fastest | allocs/op | bytes/op | note |
|---|---:|---:|---:|---|
| subtitles `rank` of 400 candidates | 7.14 ms | **257,399** | 3.26 MB | about 640 allocations per candidate, for a list the handler ranks on every request |
| subtitles `rank` of 100 candidates | 1.27 ms | 47,329 | 602 KB | |
| subtitles SRT parse, 2000 cues | 504 µs | 12,005 | 500 KB | 6 allocations per cue, 137 MB/s |
| subtitles legacy-charset download (windows-1252, 2000 cues) | 4.91 ms | 2 | 469 KB | charset detection plus transcode on every cache miss; a UTF-8 download passes through in 4.5 µs |
| subtitles serialise to VTT, 2000 cues | 562 µs | 10,801 | 140 KB | |
| subtitles cache put (disk tier), 157 KB | 110 µs | 8 | 157 KB | the body is cloned before it is written |
| remux `media` playlist, 2 h film | 144 µs | 1,202 | 43.5 KB | one `format!` per segment |
| remux subtitle-segment playlist, 2 h film | 156 µs | 1,201 | 45.8 KB | |
| remux `segments` (keyframes to segments), 2 h film | 2.14 µs | 2 | 19.2 KB | |
| remux intro/credits `longest_shared`, two 2,900-point fingerprints | 135 µs | 3 | 139 KB | |
| remux WebVTT cue parse, 1,500 cues | 407 µs | 7,503 | 295 KB | 5 allocations per cue |
| reel `/meta` JSON, 8 signed links | 10.9 µs | 228 | 16.6 KB | 29 allocations per link; unsigned is 25 |
| reel play-URL signing, key derived per call | 279 ns | 1 | 24 B | 192 ns with a reused `Signer` |
| reel cropdetect parse, 600 lines | 52.7 µs | 1,201 | 43.3 KB | 2 allocations per line |

<details><summary>All microbenchmarks (median and fastest, allocations and bytes per operation)</summary>

| benchmark | median | fastest | allocs/op | bytes allocated/op | throughput |
|---|---:|---:|---:|---:|---:|
| scout/decode_config_segment/plain | 3.68 µs | 3.53 µs | 70 | 7.05 KB |  |
| scout/decode_config_segment/sealed | 27.6 µs | 26.3 µs | 71 | 7.56 KB |  |
| scout/decode_torbox_listing | 19.6 ms | 19.4 ms | 457,788 | 4.66 MB | 536 MB/s |
| scout/memory_get_hit | 140 ns | 132 ns | 2.19 | 1.14 KB |  |
| scout/memory_get_miss | 18.2 ns | 18.1 ns |  |  |  |
| scout/memory_put_evicting | 199 ns | 120 ns | 3.17 | 1.18 KB |  |
| scout/parse_stremio_streams/streams-50.json | 58.2 µs | 57.3 µs | 1,283 | 125 KB | 183 MB/s |
| scout/parse_stremio_streams/streams-500.json | 594 µs | 587 µs | 12,756 | 1.23 MB | 180 MB/s |
| scout/pick_episode_anime24 | 276 µs | 273 µs | 465 | 51.5 KB |  |
| scout/pick_episode_scene24 | 267 µs | 262 µs | 143 | 4.61 KB |  |
| scout/pick_episode_scene200 | 2.33 ms | 2.29 ms | 1,199 | 39.1 KB |  |
| scout/pick_episode_whole_season | 6.42 ms | 6.27 ms | 3,432 | 111 KB |  |
| scout/probe_avi | 2.36 µs | 2.27 µs | 6 | 27 B |  |
| scout/probe_matroska/hi10p.mkv | 628 ns | 586 ns | 12 | 143 B |  |
| scout/probe_matroska/tracks.mkv | 524 ns | 514 ns | 12 | 143 B |  |
| scout/probe_mp4/hevc10.mp4 | 1.22 µs | 1.18 µs | 41 | 4.48 KB |  |
| scout/probe_mp4/moov-at-end.mp4 | 1.24 µs | 1.21 µs | 41 | 4.48 KB |  |
| scout/probe_mp4/multi-audio.mp4 | 1.89 µs | 1.8 µs | 57 | 6.1 KB |  |
| scout/probe_mp4/sample.mp4 | 1.18 µs | 1.16 µs | 38 | 4.38 KB |  |
| scout/rank_episode/100 | 711 µs | 703 µs | 792 | 57.1 KB |  |
| scout/rank_episode/500 | 4.94 ms | 3.66 ms | 2,760 | 123 KB |  |
| scout/rank_episode/2000 | 31.4 ms | 16.9 ms | 10,294 | 362 KB |  |
| scout/rank_movie/100 | 714 µs | 690 µs | 991 | 39.5 KB |  |
| scout/rank_movie/500 | 6.12 ms | 3.52 ms | 4,905 | 191 KB |  |
| scout/rank_movie/2000 | 29.1 ms | 14.3 ms | 19,330 | 693 KB |  |
| scout/ticket_mint | 4.07 µs | 3.99 µs | 13 | 1.3 KB |  |
| scout/ticket_open | 7.04 µs | 6.96 µs | 43 | 2.88 KB |  |
| scout/tiered_500_disk_misses | 1.14 ms | 578 µs | 1,500 | 72.5 KB |  |
| scout/tiered_get_disk_hit | 15.8 µs | 13.5 µs | 8 | 12.5 KB |  |
| scout/tiered_put_durable | 159 µs | 112 µs | 10.5 | 5.09 KB |  |
| subtitles/cache_get_mem_hit | 3.2 µs | 2.7 µs | 2 | 157 KB | 49 GB/s |
| subtitles/cache_put_disk | 124 µs | 110 µs | 8 | 157 KB | 1.26 GB/s |
| subtitles/decode_download/utf-8 | 4.58 µs | 4.5 µs |  |  |  |
| subtitles/decode_download/windows-1252 | 4.96 ms | 4.91 ms | 2 | 469 KB | 31.5 MB/s |
| subtitles/manifest | 349 ns | 307 ns | 33 | 2.42 KB |  |
| subtitles/rank_candidates/20 | 87.7 µs | 83.3 µs | 3,184 | 39.8 KB |  |
| subtitles/rank_candidates/100 | 1.43 ms | 1.27 ms | 47,329 | 602 KB |  |
| subtitles/rank_candidates/400 | 8.62 ms | 7.14 ms | 257,399 | 3.26 MB |  |
| subtitles/srt_has_a_cue | 173 ns | 169 ns | 4 | 32 B |  |
| subtitles/srt_parse/500 | 289 µs | 120 µs | 3,005 | 125 KB | 135 MB/s |
| subtitles/srt_parse/2000 | 1.15 ms | 504 µs | 12,005 | 500 KB | 137 MB/s |
| subtitles/srt_serialize/500 | 114 µs | 103 µs | 1,501 | 9.4 KB |  |
| subtitles/srt_serialize/2000 | 538 µs | 407 µs | 6,001 | 38.9 KB |  |
| subtitles/srt_serialize_vtt/500 | 159 µs | 142 µs | 2,701 | 34.6 KB |  |
| subtitles/srt_serialize_vtt/2000 | 757 µs | 562 µs | 10,801 | 140 KB |  |
| subtitles/srt_stats_and_incomplete | 1.87 µs | 583 ns |  |  |  |
| remux/auth_url_sig | 161 ns | 120 ns | 2 | 32 B |  |
| remux/auth_url_sig_ok | 161 ns | 119 ns | 2 | 32 B |  |
| remux/playlist_master | 1.03 µs | 909 ns | 14 | 1.83 KB |  |
| remux/playlist_media/2700 | 56.5 µs | 54.9 µs | 452 | 16.5 KB |  |
| remux/playlist_media/7200 | 148 µs | 144 µs | 1,202 | 43.5 KB |  |
| remux/playlist_media/14400 | 294 µs | 284 µs | 2,402 | 86.7 KB |  |
| remux/playlist_segments/2700 | 979 ns | 937 ns | 2 | 7.21 KB |  |
| remux/playlist_segments/7200 | 2.18 µs | 2.14 µs | 2 | 19.2 KB |  |
| remux/playlist_segments/14400 | 4.14 µs | 4.06 µs | 2 | 38.4 KB |  |
| remux/playlist_segments_for_start | 6.25 µs | 6.09 µs | 5 | 57.6 KB |  |
| remux/playlist_subtitle_segments | 165 µs | 156 µs | 1,201 | 45.8 KB |  |
| remux/segments_longest_shared | 150 µs | 135 µs | 3 | 139 KB |  |
| remux/subs_for_hls/200 | 996 ns | 954 ns | 1 | 90 B |  |
| remux/subs_for_hls/1500 | 7.37 µs | 7.29 µs | 1 | 90 B |  |
| remux/subs_parse_cues/200 | 57.2 µs | 55.6 µs | 1,003 | 39.2 KB | 288 MB/s |
| remux/subs_parse_cues/1500 | 415 µs | 407 µs | 7,503 | 295 KB | 301 MB/s |
| reel/build_meta_signed/1 | 2.1 µs | 1.94 µs | 39 | 3.23 KB |  |
| reel/build_meta_signed/4 | 6.19 µs | 5.98 µs | 120 | 8.98 KB |  |
| reel/build_meta_signed/8 | 11.3 µs | 10.9 µs | 228 | 16.6 KB |  |
| reel/build_meta_unsigned/1 | 1.3 µs | 1.27 µs | 35 | 2.96 KB |  |
| reel/build_meta_unsigned/4 | 3.93 µs | 3.78 µs | 107 | 7.92 KB |  |
| reel/build_meta_unsigned/8 | 7.19 µs | 6.98 µs | 203 | 14.5 KB |  |
| reel/crop_parse_all/60 | 6.03 µs | 5.45 µs | 121 | 4.38 KB | 1.35 GB/s |
| reel/crop_parse_all/600 | 56.3 µs | 52.7 µs | 1,201 | 43.3 KB | 1.46 GB/s |
| reel/crop_typical/60 | 146 ns | 144 ns | 4 | 960 B |  |
| reel/crop_typical/600 | 947 ns | 936 ns | 4 | 9.6 KB |  |
| reel/manifest | 330 ns | 319 ns | 35 | 2.42 KB |  |
| reel/sign_tag_deriving_the_key | 284 ns | 279 ns | 1 | 24 B |  |
| reel/sign_tag_with_signer | 192 ns | 186 ns | 1 | 24 B |  |
| reel/sign_verify | 296 ns | 289 ns | 1 | 24 B |  |

</details>

### HTTP: load, soak and memory

`den-hearth` release binary (the image's, amd64, so under emulation on this laptop) in a **512 MiB** container
(`--memory 512m --memory-swap 512m`, which is the cgroup the Subtitles cache is sized for), 2 worker threads, 16
concurrent clients, 10 s per case, a 60 s mixed soak, then 120 s idle. Every case passed the gate.

| case | req/s | GiB/s | p50 ms | p95 ms | p99 ms | CPU cores | RSS peak MiB | cgroup cur. peak MiB (sampled) | cgroup peak MiB (kernel, cumulative) | anon peak MiB | file peak MiB | FDs before→peak→after | TCP est. before→peak→after | Δ oom/oom_kill/max | gate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| control-health | 51,279 | 0.005 | 0.3 | 0.7 | 1.1 | 1.45 | 20 | 44.6 | 67.2 | 11.3 | 32.7 | 17→33→17 | 5→21→5 | 0/0/0 | passed |
| remux-health | 50,384 | 0.001 | 0.3 | 0.8 | 1.4 | 0.93 | 20 | 44.0 | 67.2 | 11.3 | 32.7 | 17→33→17 | 5→21→5 | 0/0/0 | passed |
| manifests | 56,708 | 0.023 | 0.3 | 0.7 | 1.0 | 1.23 | 20 | 44.1 | 67.2 | 11.3 | 32.7 | 17→33→17 | 5→21→5 | 0/0/0 | passed |
| reel-meta-warm | 49,383 | 0.034 | 0.3 | 0.7 | 1.1 | 1.49 | 20 | 44.1 | 67.2 | 11.3 | 32.7 | 17→33→17 | 5→21→5 | 0/0/0 | passed |
| reel-meta-cold | 8,249 | 0.006 | 1.9 | 3.4 | 4.3 | 1.20 | 25 | 49.0 | 67.2 | 17.4 | 32.7 | 17→62→46 | 5→50→34 | 0/0/0 | passed |
| subs-search-warm | 26,336 | 0.054 | 0.6 | 1.0 | 1.3 | 1.70 | 24 | 30.9 | 67.2 | 17.4 | 13.3 | 46→62→46 | 34→50→34 | 0/0/0 | passed |
| subs-search-cold | 6,125 | 0.013 | 2.3 | 4.1 | 7.1 | 1.44 | 125 | 373.5 | 373.9 | 117.9 | 254.8 | 46→67→50 | 34→54→38 | 0/0/0 | passed |
| subs-serve-warm | 2,190 | 0.381 | 7.2 | 11.0 | 13.9 | 1.90 | 126 | 375.5 | 375.7 | 119.6 | 254.9 | 50→66→50 | 38→54→38 | 0/0/0 | passed |
| subs-serve-cold | 453 | 0.082 | 30.3 | 68.5 | 143.6 | 1.36 | 153 | 512.0 | 512.0 | 146.4 | 380.0 | 50→67→50 | 38→54→38 | 0/0/1419 | passed |
| subs-serve-slow-readers (24) | 22 | 0.004 | 1048.6 | 1102.1 | 1126.3 | 0.07 | 133 | 512.0 | 512.0 | 131.4 | 379.8 | 50→51→50 | 38→39→38 | 0/0/1 | passed |
| remux-speed (2 MiB bodies) | 588 | 1.149 | 27.3 | 41.3 | 44.3 | 1.84 | 133 | 511.2 | 512.0 | 131.4 | 379.7 | 50→66→50 | 38→54→38 | 0/0/0 | passed |
| remux-session-flow (2 sessions, ffmpeg) | 102 | 0.018 | 101.6 | 210.5 | 513.1 | 2.43 | 174 | 512.0 | 512.0 | 155.7 | 379.7 | 50→81→51 | 38→49→39 | 0/0/203 | passed |
| slow-readers-remux-speed (24) | 2 | 0.004 | 11754.8 | 11900.1 | 11947.2 | 0.02 | 137 | 477.4 | 512.0 | 134.1 | 341.6 | 51→75→51 | 39→63→39 | 0/0/0 | passed |
| cancellation-remux-speed (32) | 4,266 | 0.000 | 6.5 | 12.5 | 17.4 | 1.99 | 138 | 159.6 | 512.0 | 134.9 | 23.6 | 51→106→51 | 39→71→39 | 0/0/0 | passed |
| cancellation-subs-slow-upstream (16) | 706 | 0.000 | 22.2 | 25.6 | 30.1 | 0.27 | 138 | 254.3 | 512.0 | 134.9 | 118.3 | 51→67→51 | 39→55→39 | 0/0/0 | passed |
| mixed-soak (ten workloads at once, 60 s) | 1,593 | 0.432 | mixed | | | 3.18 | 289 | 512.0 | 512.0 | 156.8 | 360.1 | 51→146→30 | 39→59→18 | 0/0/70 | passed |

Idle drain after the soak: 121 s idle, FDs 17→12, established TCP 5→0, process back to baseline.

Reading it:

- **cgroup memory is not RSS.** The process never needs more than about 155 MiB of anonymous memory (130 to 175 MiB of
  RSS summed over the cgroup; 289 MiB in the soak, with two ffmpeg processes), but `memory.current` reaches the **512 MiB
  limit** in every case after `subs-serve-cold`: 380 MiB of it is page
  cache from Subtitles' disk tier. The kernel reclaims it (`memory.events max` = 1,419 in that one case) and
  nothing is OOM-killed (`oom`, `oom_kill` stay 0 throughout), so an alert on `memory.current` close to the limit would
  fire on a healthy node. The signals that mean something are `oom_kill`, `anon`, and RSS.
- **Subtitles dominates memory.** Idle the process holds 20 MiB RSS (11 MiB anon). After a cold-search run its
  anonymous memory is 118 MiB, and 146 MiB after cold downloads, against a 64 MiB memory-tier budget
  (`MAX_MEMORY_BYTES`): about 2x the budget. The likely cause is glibc keeping freed 100 KB bodies in its arenas,
  which is an inference (nothing here profiled the allocator). These figures are glibc's; hearth now ships jemalloc,
  which holds less (see [Allocators](#allocators)). The gate's `RUN_CACHE_FILL` allowance (160 MiB) is the glibc
  measurement plus margin.
- **Remux with ffmpeg**: two sessions of the 25 s sample keep the cgroup at 3 processes (hearth plus an ffmpeg per
  session), 174 MiB RSS summed, 2.4 cores, and serve about 100 segment requests a second at p50 102 ms (a segment is
  waited for until ffmpeg writes it). Both sessions end with 204 and the process count returns to 1.
- **Idle upstream connections stay pooled** (reqwest keeps them 90 s): cold cases leave 34 to 50 established sockets
  and as many descriptors until the pool expires. The gate allows that per case and requires the exact baseline after
  the final idle drain, which it reached in 121 s.
- **Small answers are server-thread-bound**: `/health` and cached `/meta` run at 50,000 requests a second at p99 about
  1 ms using 1.5 of the 2 worker threads. The Python driver (4 processes) is about as saturated, so these are lower
  bounds on capacity.
- **Cancellation storms are clean.** 4,266 cancelled 8 MiB downloads a second leave descriptors, sockets and memory at
  baseline. A cancelled subtitle download is not aborted upstream (the download is a request-independent worker that
  fills the cache for the next viewer): the stub saw those downloads complete.

### HTTP overload: bounded admission

The `overload` case (`bench/http/run.py --only overload`) holds 188 clients on the node for 20 s past what it admits,
on the main port **and on the four old service ports** (Scout 8080, Reel 8092, Subtitles 8093, Remux 8095): 64 + 32
Remux media clients and 40 slow readers (against 16 Remux media permits), 32 cold Reel `/meta` and 8 Scout stream
requests (against 48 bulk positions), and small control clients on every port. The gate requires no client error, only
the statuses and refusals the case names, every 503 carrying `Retry-After`, no `oom`/`oom_kill`/`max`, and memory,
descriptors and sockets back to where they started. Two runs of each image back to back, same machine (the VM was shared
with other benchmark runs, so absolute numbers move between pairs; compare within a pair). Before is `main` (1.2.0),
after is this PR. Raw rows: `bench/results/http-overload-*.jsonl`.

| | main run 1 | run 2 | this PR run 1 | run 2 |
|---|---:|---:|---:|---:|
| `/health` :8090 p50 / p99 ms | 200 / 361 | 204 / 418 | **3.9 / 31.5** | **1.4 / 10.5** |
| Remux `/remux/health` :8095 p99 ms | 351 | 455 | 31.4 | 10.4 |
| Scout `/health` :8080 p99 ms | 379 | 462 | 32.0 | 10.9 |
| Reel `/manifest.json` :8092 p99 ms | 427 | 504 | 26.8 | 10.8 |
| Subtitles `/manifest.json` :8093 p99 ms | 364 | 454 | 27.5 | 10.9 |
| Reel cold `/meta` :8092 p99 ms | 1,187 | 1,267 | 84.8 | 34.0 |
| Scout bulk :8080 p99 ms | 383 | 431 | 27.7 | 12.7 |
| slow readers p50 | 11.2 s (all served) | 11.1 s | 31 ms (refused) | 26 ms |
| statuses | 200: 11,719 · 400: 810 | 200: 10,923 · 400: 738 | 200: 72,573 · 400: 29,899 · **503: 206,352** | 200: 181,771 · 400: 64,848 · **503: 396,738** |
| 503s without `Retry-After` | n/a | n/a | 0 | 0 |
| media bodies completed (main + :8095 + slow) | 9,582 | 8,944 | 631 | 729 |
| anon peak MiB | 41.9 | 49.0 | 30.9 | 30.9 |
| unreclaimable growth after recovery | **+27.8 MiB (gate failed)** | **+36.3 MiB (gate failed)** | passed | passed |
| `oom` / `oom_kill` / `max` | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 |
| CPU cores | 1.55 | 1.55 | 0.70 | 1.05 |

Reading it:

- **Control traffic stays fast.** On main a `/health` probe waits behind the flood like everything else (p50 about 200
  ms, p99 360 to 505 ms, on every port); with this PR it is 1 to 4 ms at p50 and 10 to 32 ms at p99 on every port,
  because the 16 control positions are not reachable by bulk and the media flood is refused before it costs a thing.
- **Excess is refused, not queued**: 200,000 to 400,000 `503 {"error":"media_busy"}` with `Retry-After: 1`, in about 1
  ms each. The cost is that a client that cannot get a permit gets no body: the 16 Remux permits are held by whoever
  holds them (here mostly the slow readers, which keep a permit for as long as they keep reading), so the fast clients
  completed far fewer bodies than before. That is the intended trade (a player retries; a slow reader should not
  take the node down). The slow readers here are not stalled, they keep moving bytes (4 KiB per 20 ms), so the idle
  clock never fires on them: they hold a permit for the 10 s it takes to read 2 MiB, which is a fairness limit of 16
  permits and not a deadline problem. A reader that stops entirely is cut at the idle deadline (Remux 10 s, Reel 30 s;
  unit test `remux_has_a_shorter_idle_deadline_than_reel`).
- **Memory is bounded, not dramatic, in this harness.** The 188 clients cannot offer more than they hold, so main
  also stays inside 512 MiB (`oom` 0). What differs is the anonymous peak (about 25% lower) and that main does
  not return to its starting unreclaimable memory within the gate's window (the large freed bodies sit
  in glibc's arenas) where this PR does. `memory.stat`'s `sock` reads 0 on this VM's cgroup, so the socket-buffer
  ceilings (`sock.rs`) are proven by unit test (`a_writer_to_a_client_that_never_reads_is_stopped_by_the_ceiling`), not by
  this case.
- **The send queue and distance.** The numbers above were measured with the first send ceiling (64 KiB) and no
  harness case has a long round trip, so they say nothing about it. A connection has about its send queue in flight, so
  throughput is capped near `queue / RTT`: 64 KiB gives ~10 Mbit/s at 50 ms (below a 4K remux), the default 256 KiB
  ~42 Mbit/s at 50 ms and ~21 Mbit/s at 100 ms. `HEARTH_SNDBUF` (bytes, at least 16 KiB) overrides it, and the kernel
  clamps the request to `net.core.wmem_max` (208 KiB by default), so a node serving remote 4K clients raises both.
  Not measured here; the arithmetic is the argument.
- **Connections** peaked at 250 to 268 established, the 188 clients plus the idle upstream pool, so this case stays
  under the 256 cap on purpose. The next case exceeds it.

The full suite passes on this PR (`bench/results/http-20261014T125516Z-40620.jsonl`, 18 cases and the idle drain):
`slow-readers-remux-speed` now sees 503s for the readers past the permits, `remux-speed` may see the kernel reclaim
Subtitles' page cache (`allow_max`; it moved by 0 to 16 across runs of the same image), `remux_flow.py` retries a refused
init section as a player does, and every other row is within the noise of the baseline above.

### HTTP connection flood: a full pool must not shut out a new connection

`bench/http/run.py --only connection-flood`: 320 busy keep-alive clients (more than the 256 connections the node accepts)
hammering a bulk route for 20 s, and two kinds of probe that each open a **fresh connection** every time: 4 clients on the
driver, and a loop inside the hearth container on `127.0.0.1`, which is what the image's `HEALTHCHECK` and den-update's
probe do. Three images: `main` (no cap), the first version of this PR (cap of 256, no reserve: a new connection waits in
the listen backlog), and this PR (the cap plus 64 probation connections: past the cap a peer is accepted, may ask control
routes only, gets one answer and is closed; anything else is a `503 server_busy`). Two runs of each capped image.
Raw rows: `bench/results/http-connflood-*.jsonl`.

| | main (no cap) | cap only, run 1 | cap only, run 2 | this PR run 1 | run 2 |
|---|---:|---:|---:|---:|---:|
| fresh-connection `/health` from the driver: completed in 20 s | 90,846 | 12,797 | 20,799 | 74,233 | 68,386 |
| ...of which timed out (10 s) | 0 | **4** | **4** | 0 | 0 |
| loopback `/health` in the container: completed / timed out (5 s) | 375 / 0 | 91 / **3** | 89 / **3** | 380 / 0 | 381 / 0 |
| loopback p99 ms | 46.5 | 44.6 | 170.9 | 13.6 | 8.5 |
| the flood's own statuses | all 200 | all 200 (excess silent in the backlog) | all 200 | 200 and 9,692 `503 server_busy` | 200 and 9,430 `503 server_busy` |
| gate | passed | **failed** (timeouts) | **failed** | passed | passed |

With the cap alone, a probe that arrives while every position is held waits behind the flood's own reconnects: seven
probes per run sat for the full timeout and the probe loops completed a fraction of their requests. With probation no
probe timed out in either run and the probe rates are back to main's. Probation is a bounded, short-lived addition (64
connections, a 1 s head timeout, no keep-alive, at most 2 MiB of read buffer), and it also turns the flood's overflow into
the stable 503 instead of silence. The first attempt had 16 slots and a 2 s head timeout and still dropped 4 probes in 20
s: an overflowing client holds a slot until it has sent its head, so the slots must outnumber a flood that reconnects as
fast as it is refused.

### HTTP without the container (native, CPU path)

The release binary directly on the Mac, with the same stubs and driver on loopback, no memory cap (so no gate), no
Remux session case. `cargo build --release -p den-hearth --features stub-upstreams`, then `bench/http/native.py`.

| case | req/s | GiB/s | p50 ms | p99 ms | CPU cores | RSS MiB |
|---|---:|---:|---:|---:|---:|---:|
| control-health | 50,330 | 0.010 | 0.3 | 0.9 | 1.41 | 5.8 |
| manifests | 42,980 | 0.018 | 0.4 | 1.1 | 1.09 | 6.0 |
| reel-meta-warm | 41,436 | 0.030 | 0.4 | 1.1 | 1.45 | 7.1 |
| reel-meta-cold | 5,433 | 0.004 | 2.7 | 8.4 | 0.64 | 14.8 |
| subs-search-warm | 26,044 | 0.054 | 0.6 | 1.8 | 1.86 | 15.3 |
| subs-search-cold | 5,117 | 0.011 | 2.9 | 8.0 | 1.65 | 100.3 |
| subs-serve-warm | 1,758 | 0.306 | 8.0 | 29.1 | 1.63 | 49.6 |
| subs-serve-cold | 153 | 0.027 | 90.9 | 254.2 | 0.87 | 131.8 |
| remux-speed | 160 | 0.313 | 99.1 | 189.8 | 1.47 | 131.9 |

The emulated amd64 container is within noise of native arm64 on the CPU-bound cases; the byte-moving cases are faster in
the container because Linux's loopback and veth are faster than macOS's loopback.

### Allocators

Does another global allocator, or glibc tuning, pay for itself ([#282](https://github.com/oxyc/den/issues/282))? Four
builds of the same code through the container run above, six full runs each (24 in all, order rotated; the closed
experiment is [#22](https://github.com/oxyc/den-hearth/pull/22), branch `perf/allocator`, with the per-run tables in
`bench/results/allocator/tables.md`): glibc, glibc with `MALLOC_ARENA_MAX=2`, mimalloc 0.1.52, and jemalloc 5.3.1
(`tikv-jemallocator` 0.7) tuned with `background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000,lg_tcache_max:16`
(the last is clipped to jemalloc's default of 32 KiB on a 4 KiB-page host, so it did nothing here and is not shipped).
Median of six, MiB, range in brackets (cgroup `anon` is hearth plus the ffmpeg children it spawns; no OOM kill in any run):

| | glibc | glibc-arena2 | mimalloc | jemalloc |
|---|---:|---:|---:|---:|
| anon peak, any case | 152 (144 to 168) | 154 (140 to 170) | 159 (146 to 165) | **135 (129 to 138)** |
| anon after the 121 s idle drain | 123 (115 to 145) | 127 (114 to 148) | 123 (116 to 134) | **98 (94 to 100)** |
| RSS after the idle drain | 124 | 129 | 124 | **99** |
| anon peak, `reel-meta-warm` (an untouched process) | 12 | 12 | 21 to 39 | 15 |
| gate failures | 2 of 6 | 1 of 6 | 5 of 6 | 0 of 6 |
| image size | 775.32 MB | same | +0.18 MB | +0.57 MB |

**Why jemalloc.** It is the only candidate that moved memory: 20% less anon after idle and 11% less at peak, its worst
run better than glibc's best (100 against 115 at idle), no gate misses, for 0.57 MB of image. `MALLOC_ARENA_MAX=2` does
nothing for memory, and mimalloc keeps more than glibc (an untouched process holds 21 to 39 MiB) and missed the "memory
back within 8 MiB of the case start" gate in `remux-session-flow` in three runs. Throughput did not separate them:
requests per second moved by up to 2x between runs on a laptop with a load average from 2.6 to 27, and every candidate,
glibc itself included, "beat" glibc by 1.3x to 1.5x on the control case that allocates next to nothing. Single-thread
hot loops gain at most 1.4x (Subtitles' ranking, 640 allocations per candidate). The win is 25 MiB, not 2x, and the
page cache (380 of the 512 MiB) dwarfs it; it is adopted as a clear memory win with no throughput loss the noise can
show.

**How it is applied.** `crates/hearth/src/alloc.rs` installs jemalloc as the global allocator and exports the tuning
as `_rjem_malloc_conf`, which jemalloc reads before its first allocation, so no environment is needed; an operator's
`_RJEM_MALLOC_CONF` overrides it. The startup line `allocator: jemalloc ...` prints what jemalloc reports, and a unit
test reads the same settings back. (Verified in the image: `background_thread=true dirty_decay_ms=1000
muzzy_decay_ms=1000`, and the same line with an override in the environment.) Nothing returns to the 11 MiB baseline for any allocator: after the drain the process
holds about 16 MiB more than its caches explain with jemalloc, about 40 MiB more with glibc (an inference from the
declared cache sizes; nothing walked the heap).

**Caveats.** amd64 under emulation on a loaded laptop, not the box; the microbenchmarks were aarch64; fragmentation was
not measured directly.

### Test suite

`cargo test --workspace --no-fail-fast`, Apple M4, debug profile, commit `1e507b4`, load average 5.5 at the start.

| step | seconds |
|---|---:|
| clean build of every test binary | 29.96 |
| run every test (warm build) | 21.46 |
| incremental: touch `crates/scout/src/util.rs` | 1.35 |
| incremental: touch `crates/remux/src/redact.rs` | 1.61 |
| incremental: touch `crates/hearth/src/route.rs` | 1.14 |

| test binary | result | passed | failed | ignored | seconds |
|---|---:|---:|---:|---:|---:|
| den-hearth | ok | 12 | 0 | 0 | 0.01 |
| den-reel | ok | 286 | 0 | 6 | 5.41 |
| den-remux | ok | 245 | 0 | 17 | 2.68 |
| den-scout | **FAILED** | 460 | 43 | 162 | 6.81 |
| den-subtitles | ok | 288 | 0 | 0 | 2.55 |

den-scout's 43 failures were the handler tests: when this was measured `handler.rs` was still a `todo!()` stub. Scout's
handler has since landed and `cargo test --workspace` passes in full (662 den-scout tests), so this table's den-scout row
is historical; re-measure with `bench/suite-timing.sh`.

The slowest 20 tests (debug build; the scout ones are regex compilation at `opt-level 0`):

| # | test | seconds |
|---|---|---:|
| 1 | den-scout: tests::rank::lookaround_patterns_answer_for_a_title_of_any_length | 6.587 |
| 2 | den-reel: tests::a_lingering_descendant_does_not_pin_a_finished_download | 5.155 |
| 3 | den-remux: source::tests::growing_ranges_need_far_fewer_round_trips_on_a_controlled_high_bdp_path | 2.668 |
| 4 | den-subtitles: srt::tests::escaping_a_hostile_line_stays_linear | 2.505 |
| 5 | den-remux: source::tests::a_read_paused_longer_than_any_total_timeout_carries_on | 2.132 |
| 6 | den-scout: tests::stores_pick::episode_patterns_bare_ranges_span_a_whole_season | 1.946 |
| 7 | den-scout: tests::stores_pick::episode_patterns_a_tight_dash_is_not_enough_to_make_a_range | 1.926 |
| 8 | den-scout: tests::stores_pick::episode_patterns_the_range_second_element_must_be_an_episode | 1.91 |
| 9 | den-scout: tests::stores_pick::cached_episode_patterns_answers_for_the_episode_asked_about | 1.872 |
| 10 | den-scout: tests::stores_pick::names_episode_a_range_holds_every_episode_in_it | 1.828 |
| 11 | den-scout: tests::stores_pick::names_episode_reads_every_episode_in_a_chain | 1.626 |
| 12 | den-scout: tests::stores_pick::pick_episode_file_a_bare_number_never_outranks_a_label | 1.484 |
| 13 | den-scout: tests::stores_pick::pick_episode_file_a_title_number_is_not_an_episode | 1.426 |
| 14 | den-reel: tests::concurrent_asks_for_one_id_share_a_single_resolve | 1.192 |
| 15 | den-scout: tests::scrape::scraper_url | 1.131 |
| 16 | den-subtitles: cache::tests::the_sweep_bounds_the_disk_tier | 1.112 |
| 17 | den-subtitles: cache::tests::the_sweep_spends_the_cheap_entries_first | 1.107 |
| 18 | den-subtitles: opensubtitles::download_tests::a_rate_limit_on_one_title_holds_every_title_until_an_answer | 1.104 |
| 19 | den-scout: tests::stores_pick::pick_episode_file_bare_numbered_packs | 0.961 |
| 20 | den-reel: tests::duplicates_of_one_title_do_not_spend_the_prewarm_cap | 0.827 |

Everything outside these twenty finishes in under 0.83 s. Cargo runs the test binaries one after another and each binary
is as long as its slowest test, so the 21 s is roughly the sum of the five. Eleven of the twenty are den-scout's, and
nine of those are the episode-matching tests; going by what they call, their time is regex construction at `opt-level 0`
(not profiled), so an optimisation of episode matching would speed them up too.

### Test-suite speed

`bench/test_speed.py` measures a checkout three times per case and reports wall AND CPU seconds (user+sys of the whole
process tree). The machine here was shared (load average 25-30 throughout), so wall time is mostly the neighbours; CPU
seconds are what to compare. Before = `ae0fc1d`, after = this branch. Raw lines: `bench/results/test-speed/`.

| case (min of 3) | before wall / CPU s | after wall / CPU s |
|---|---:|---:|
| clean build of every test binary | 100.4 / 313.3 | 65.4 / 254.1 |
| `cargo test --workspace`, warm | 26.7 / 25.1 | 15.5 / 12.6 |
| `cargo nextest run --workspace`, warm | 13.3 / 45.1 | 21.2 / 38.4 |
| incremental: touch `crates/scout/src/util.rs` | 2.1 / 4.7 | 7.7 / 6.7 |
| incremental: touch `crates/remux/src/redact.rs` | 1.5 / 4.2 | 3.1 / 4.9 |

What moved: warm `cargo test` CPU halves and the clean build loses a fifth of its CPU. The incremental rebuild costs
about 2 s more CPU for scout (0.7 s for remux): the optimised dependencies make the test binary slower to link. nextest
is for isolation and per-test timeouts, not speed: one process per test costs about three times the CPU of `cargo
test`, so its wall time depends on idle cores (13 s on a quiet minute, 21-46 s under load).

Slowest tests, one run each, wall seconds (concurrent with the rest of the suite, so inflated):

| test | before | after |
|---|---:|---:|
| den-scout `rank::lookaround_patterns_answer_for_a_title_of_any_length` | 13.45 | 4.27 |
| den-subtitles `srt::tests::escaping_a_hostile_line_stays_linear` | 5.43 | 3.16 |
| den-reel `a_lingering_descendant_does_not_pin_a_finished_download` | 5.16 | 5.17 |
| den-scout `stores_pick::cached_episode_patterns_answers_for_the_episode_asked_about` | 4.03 | below 1.1 |
| den-remux `growing_ranges_need_far_fewer_round_trips_…` | 2.53 | 2.66 |
| den-remux `a_read_paused_longer_than_any_total_timeout_carries_on` | 2.51 | 2.10 |

The scout regex tests are the clean measurement: run alone, the lookaround and cached-episode tests retired 107.6 G
instructions before and 34.6 G after (`/usr/bin/time -l`); the whole scout test binary went from 166.5 G to 86.6 G.

Rejected, with numbers:

- `[profile.dev.package.den-scout] opt-level = 1`: the two regex tests fall from 34.6 G to 15.1 G instructions, but the
  crate takes about 2 minutes to build instead of 20-30 s, on every edit.
- lld (`-C link-arg=-fuse-ld=lld`, nixpkgs lld 21): incremental CPU 6.0/5.1 s against 6.5/5.4 s, inside the noise, and
  it needs a tool nobody has installed.
- `split-debuginfo`: `unpacked` is already the macOS default for the dev profile.
- Paused time for the real-socket tests in remux and reel: reqwest's timeouts would fire on virtual time while bytes are
  still in flight, which changes what the test proves. Only the two in-process `Flight` tests in scout took
  `start_paused`; the other scout sleeps (5-100 ms each, mock `Doer`, no sockets) are already paused or too short to
  matter.
- `escaping_a_hostile_line_stays_linear` runs den-subtitles' own code, not a dependency's, so the dependency settings do
  not touch it (41.6 G instructions either way). It stays as it is.

Run it in CI (and locally):

```sh
cargo test --workspace --locked --no-fail-fast                                   # includes doctests
nix run nixpkgs#cargo-nextest -- nextest run --workspace --locked --profile ci   # isolated, per-test timeouts (.config/nextest.toml)
python3 bench/test_speed.py --label after --root . --target /tmp/target --runs 3 # the table above
```

### One cache tier (den-cache)

Scout and Subtitles used to carry two different tiered caches; both are now `crates/cache`, built from Scout's design.
Measured by replaying household traffic against the real cache on a real directory with a simulated clock
(`crates/subtitles/benches/cache_replay.rs`, `crates/scout/benches/cache_replay.rs`; the Subtitles replay also counts heap with
a counting allocator). Before = `58d32df` (baseline hooks only), after = this branch. Raw lines:
`bench/results/den-cache/`. Subtitles, 24 h / 400 titles and 72 h / 150 titles:

| | before | after |
|---|---:|---:|
| disk writes per hour | 144.1 / 86.5 | 139.2 / 82.0 |
| bytes written per hour | 4,629,736 / 2,379,720 | 4,629,638 / 2,379,631 |
| hit rate | 0.376 / 0.493 | 0.376 / 0.493 |
| heap with the cache full (MB) | 69.5 / 70.8 | 70.7 / 70.7 |
| hourly sweep (ms, 2.8-3.7 k files) | 14-38 / 34-63 | 0.9 / 3.1-3.4 |
| restart over the same directory, 24 h run (ms; fastest / median of 4) | 35.0 / 37.2 | 36.5 / 36.8 |

In this replay the writes it saves are the pin that is written twice in a row (the same value, 180 days): the search
entries, quota counters and bodies all change, or are new, so there is nothing else to skip. Bytes do not move because
those files are 5-10 bytes. Opening the cache walks the directory once: that one pass builds the index and reclaims what
expired, so the boot sweep is not a second pass (an earlier version of this branch read the directory twice and was
slower to start). Restart runs were interleaved, before/after, at load average 17-28 on a shared laptop; the first run
after a build is a 5-10x cold-cache outlier on either side. The sweep time and restart rows for 72 h come from earlier,
two-pass runs and are in the raw lines only.

Scout, 24 h: writes 256.8 /h and 362 skipped puts, unchanged; bytes 2,162,663 -> 2,163,849 /h (the length in the
header); hit rate 0.627 unchanged; restart over 4,352 files (fastest / median of 3) 61.2 / 78.1 ms before, 58.8 / 59.5
ms after, one directory listing in both.

Divan, `bench/micro.sh subtitles cache_` (median / mean ns, a cache holding about 64 MiB):

| | before | after |
|---|---:|---:|
| hit, 900 bodies of 70 KB | 123 / 120 | 69 / 78 |
| hit, 30,000 markers of 2 KB | 412 / 499 | 214 / 245 |
| put that evicts, 900 bodies | 2,361 / 2,365 | 1,153 / 1,349 |
| put that evicts, 30,000 markers | 60,260 / 86,800 | 188 / 207 |
| put with a disk write (157 KB) | 135,000 / 199,000 | 142,000 / 145,000 |
| `get_shared` hit, 64 bodies | 45.5 | 51.5 |

The eviction was a scan of every entry for the least recently used; a put into a full cache of small entries now costs
what an insert costs. Scout's memory tier is the same code as before, and `cache_mem` `get_hit` t=1 reads 21.6 ns before and
22.5 ns after (this laptop was shared, and the same binary reads 20-50 ns from run to run).

## How to run

```sh
bench/micro.sh                  # all four crates -> bench/results/micro-<stamp>.jsonl; ~2 min
bench/micro.sh scout rank       # one crate, benchmarks whose name contains "rank"
bench/micro.sh --test           # each benchmark once: the quick "do they still run" check
bench/go/run.sh                 # den-scout's Go side, ~4 min; needs nix (or a Go on PATH) and a den-scout checkout
bench/http/run.py               # the enforced container run, ~10 min; needs Docker or Podman
bench/http/run.py --list        # the case names; --only a,b runs a subset (and is never published as complete)
bench/http/native.py            # after: cargo build --release -p den-hearth --features stub-upstreams
bench/suite-timing.sh           # cargo clean, then clean/incremental/per-test timing; ~2 min
python3 bench/http/test.py      # the gate's and driver's own tests (no container)
python3 bench/summarize.py micro|compare|http|suite <file.jsonl>     # the tables above
```

`bench/micro.sh` and `bench/go/run.sh` write the fixtures with `bench/gen_fixtures.py` first (seeded; same bytes on every
machine, in `bench/fixtures/`, git-ignored). The Go run reads `DEN_SCOUT=<checkout>` (default
`~/Projects/Personal/den-scout`) only through a throwaway `git worktree` of its `origin/main`, removed afterwards, and
takes `COUNT` runs per benchmark.

Environment for the HTTP run: `MEMORY=512m` (the limit; swap is set to the same), `DURATION=10`, `SOAK_DURATION=60`,
`CONCURRENCY=16`, `HEARTH_WORKER_THREADS=2`, `PROCS=4` (driver processes), `RECOVERY_TIMEOUT=20`, `DRAIN_TIMEOUT=150`,
`IMAGE=den-hearth:bench`, `PLATFORM=linux/amd64`, `CONTAINER_ENGINE=docker`. If the image does not exist it is built from
the repo's `Dockerfile` with `--build-arg DEN_VERSION=$(cat VERSION) --build-arg HEARTH_FEATURES=stub-upstreams`; the
first build compiles GPAC, ffmpeg and alass from source (about 25 minutes under emulation), later ones reuse the layers.
Exit 77 means no container engine was available, so a misleading host-only run was not made.

## The HTTP harness

```
driver    load.py / remux_flow.py against hearth:8090          python:3.13-slim, native
hearth    the release binary, --memory M --memory-swap M       the container under test (the repo Dockerfile's image)
stub      TMDB, KinoCheck, OpenSubtitles + CDN, scout, debrid  bench/http/stubs.py, native, 4 worker processes
observer  --pid=host --cgroupns=host --privileged              reads hearth's cgroup and /proc from outside, so measuring costs it nothing
```

All four sit on one `docker network create --internal` network, which has no route out; the run proves it (a connection to
1.1.1.1 from the hearth container must fail) before it starts. TMDB, KinoCheck and OpenSubtitles are hard-coded in the
services, so the binary is built with **`--features stub-upstreams`**: `crates/hearth` then reads `HEARTH_STUB_TMDB`,
`HEARTH_STUB_KINOCHECK` and `HEARTH_STUB_OPENSUBTITLES`. The feature is off in the shipped image (the `Dockerfile`'s new
`HEARTH_FEATURES` build argument is empty by default), and it has to stay off: a base taken from the environment would let
whoever sets it collect the TMDB and OpenSubtitles keys. Remux needs no seam: its scout and subtitle origins are
configuration, and its sample (`crates/remux/testdata/h264.mkv`) is served by the stub with `Range`.

The cases are `bench/http/run.py --list`: control and manifest JSON, Reel `/meta` warm and cold (unique ids, so every
request asks the stub), Subtitles search and file serve warm and cold, slow readers, Remux speed bytes, a real
**Remux session flow** (`POST /remux/session` over the sample, then master, media, init and segments at the pace ffmpeg
makes them, then `DELETE`), cancellation storms, and a 60 s mixed soak with all of them at once. Each case is
probe, load with a 250 ms cgroup sampler, probe, wait for `/health` and for resources to settle, probe, gate.

**The gate** (`bench/http/gate.py`, tested by `bench/http/test.py` including each rejection) fails the run when:

- any client sees an error (reset, truncated body, `Content-Length` mismatch, unframed 200, wrong size or prefix), a
  status the case does not allow, or a refusal with a code it does not allow; a required status never appears; or too few
  requests complete;
- `memory.events` `oom`, `oom_kill` or `oom_group_kill` moves, or `max` moves in a case that does not write files;
  or the container does not start with `memory.max` exactly the limit, `memory.swap.max` 0 and zero OOM events;
- `/health` does not answer after the case, or, within the recovery timeout, anonymous + kernel + socket memory is not
  back within 8 MiB of the case's start and of the run's baseline (plus the declared cache fill), descriptors within 2,
  established TCP sockets, sockets to the upstream stub and cgroup processes (an ffmpeg left behind) not exactly back
  (plus the declared idle-pool allowance), and, after the whole run has idled past the pool's timeout, not exactly back
  to baseline;
- a cancellation case sees anything but cancelled requests.

Page cache is excluded from the memory recovery check on purpose (it is reclaimable and rises with any disk I/O);
`memory.current`, the sampled peak and the kernel's `memory.peak` are in every row. A failing run keeps its artifacts
(`bench/results/failed-<dir>/`: the report so far, every case's samples, hearth's and the stub's logs); only a run that
reaches the terminal `{"gate":"passed","complete":true}` row is renamed from its hidden `.partial` into a visible
`http-<stamp>.jsonl`.

## What is not measured

- **Reel `/progressive`'s index and the moov rebuild** (the 630 ms item in #282): it needs a fragmented-MP4 fixture, and
  the code that builds one is a test helper inside the crate. A follow-up PR adds the fixture with the optimisation.
- **Remux probing and the GPU transcode**: the probes read over HTTP through a private `Source`, and a transcode needs the
  box's VAAPI device. The session flow does cover the real ffmpeg copy/AAC path over the sample.
- **Subtitles sync and translation** (alass, LLMs): external tools and paid APIs.
- **den-scout's real work over HTTP**: Scout is in the hearth binary and the `overload` case drives its port, but only
  its cheap answers (`/health`, a refused `/stream`); a real stream list needs indexers and a debrid service, which the
  harness stubs do not model. The scout microbenchmarks are the pure functions behind the handler.
- **1 and 10 Gbit shaping** (den-edge's `tc` cases): it needs a dedicated veth and root. The bytes cases above measure the
  unshaped path.
- **Real network and real debrid.** By design.
- **A quiet machine.** These numbers come from a shared laptop. The harness, the fixtures and the commands are what
  carry over: run them on the target host before holding an optimisation to a number. This PR ran nothing on the homelab
  box.

## Updating the baselines

Run the harnesses on the same machine, commit the new `bench/results/*.jsonl`, regenerate the tables with
`bench/summarize.py` and replace them here, with the date, commit and `meta` load. Keep one result file per kind: the
older ones are in git history. A PR that claims a speed-up should quote the `fastest` and the allocation count from a
before/after pair of `bench/micro.sh` runs taken back to back.
