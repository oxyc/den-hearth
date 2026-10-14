#!/usr/bin/env python3
"""Markdown tables from the JSONL results, so BENCHMARKS.md and a PR body are generated, not retyped.

    summarize.py micro  results/micro-<stamp>.jsonl            allocation-counted divan results
    summarize.py compare results/micro-<stamp>.jsonl results/go-<stamp>.jsonl     Rust vs Go, side by side
    summarize.py http   results/http-<stamp>.jsonl             the enforced load run
    summarize.py suite  results/suite-timing-<stamp>.jsonl     test-suite timing
"""
import json
import sys

GO_NAME = {
    "rank_movie/": "Rank/movie/", "rank_episode/": "Rank/episode/",
    "pick_episode_scene24": "PickEpisode_scene24", "pick_episode_anime24": "PickEpisode_anime24",
    "pick_episode_scene200": "PickEpisode_scene200", "pick_episode_whole_season": "PickEpisode_wholeSeason",
    "parse_stremio_streams/": "ParseStremioStreams/", "decode_torbox_listing": "DecodeListing",
    "decode_config_segment/": "DecodeConfig/", "ticket_mint": "Ticket/mint", "ticket_open": "Ticket/open",
    "probe_matroska/": "Probe/", "probe_mp4/": "Probe/", "probe_avi": "Probe/xvid.avi",
    "memory_put_evicting": "MemoryCache/put_evicting", "memory_get_hit": "MemoryCache/get_hit",
    "memory_get_miss": "MemoryCache/get_miss", "tiered_put_durable": "TieredCache/put_durable",
    "tiered_get_disk_hit": "TieredCache/get_disk_hit", "tiered_500_disk_misses": "TieredCache_500DiskMisses",
}


def rows(path):
    return [json.loads(l) for l in open(path) if l.strip()]


def t(ns):
    if ns is None:
        return ""
    for unit, scale in (("s", 1e9), ("ms", 1e6), ("µs", 1e3)):
        if ns >= scale:
            return f"{ns / scale:.3g} {unit}"
    return f"{ns:.3g} ns"


def n(value):
    return "" if value is None else f"{value:,.0f}" if value >= 100 else f"{value:.3g}"


def size(b):
    if b is None:
        return ""
    for unit, scale in (("MB", 1e6), ("KB", 1e3)):
        if b >= scale:
            return f"{b / scale:.3g} {unit}"
    return f"{b:.0f} B"


def table(header, body):
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" if i == 0 else "---:" for i, _ in enumerate(header)) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in body]
    return "\n".join(out)


def micro(path):
    data = [r for r in rows(path) if r.get("kind") != "meta"]
    body = []
    for r in data:
        body.append((f"{r['suite']}/{r['bench']}", t(r["median_ns"]), t(r["fastest_ns"]), n(r.get("alloc_count_median")),
                     size(r.get("alloc_bytes_median")), size(r.get("throughput_median_bytes_s")) + ("/s" if r.get("throughput_median_bytes_s") else "")))
    print(table(["benchmark", "median", "fastest", "allocs/op", "bytes allocated/op", "throughput"], body))


def go_key(rust):
    for prefix, go in GO_NAME.items():
        if rust == prefix.rstrip("/") or (prefix.endswith("/") and rust.startswith(prefix)):
            tail = rust[len(prefix):] if prefix.endswith("/") else ""
            if prefix in ("probe_matroska/", "probe_mp4/"):
                return "Probe/" + tail
            return go + tail
    return None


def compare(rust_path, go_path):
    go = {r["bench"]: r for r in rows(go_path) if r.get("kind") != "meta"}
    body = []
    for r in rows(rust_path):
        if r.get("kind") == "meta" or r["suite"] != "scout":
            continue
        key = go_key(r["bench"])
        g = go.get(key) if key else None
        if not g:
            continue
        body.append((r["bench"], t(r["fastest_ns"]), t(g["fastest_ns"]), f"{g['fastest_ns'] / r['fastest_ns']:.1f}x",
                     t(r["median_ns"]), t(g["median_ns"]), f"{g['median_ns'] / r['median_ns']:.1f}x",
                     n(r.get("alloc_count_median")), n(g.get("alloc_count_median")),
                     size(r.get("alloc_bytes_median")), size(g.get("alloc_bytes_median"))))
    print(table(["benchmark", "Rust fastest", "Go fastest", "Go / Rust", "Rust median", "Go median", "Go / Rust",
                 "Rust allocs/op", "Go allocs/op", "Rust B/op", "Go B/op"], body))


def mib(b):
    return "" if b is None else f"{b / (1 << 20):.1f}"


def http(path):
    data = rows(path)
    meta = next(r for r in data if r.get("memory"))
    cases = [r for r in data if r.get("case") and r["case"] != "idle-drain" and "req_s" in r]
    body = []
    for r in cases:
        cg = r["cgroup"]
        ev = cg["events_delta"]
        body.append((r["case"], f"{r['req_s']:,.0f}", f"{r['gib_s']:.3f}", r["p50_ms"] if r["p50_ms"] is not None else "mixed",
                     r["p95_ms"] if r["p95_ms"] is not None else "", r["p99_ms"] if r["p99_ms"] is not None else "",
                     f"{r['cpu_cores']:.2f}", f"{r['rss_kib']['peak'] / 1024:.0f}", mib(cg["current_peak_sampled"]),
                     mib(cg["peak_kernel_cumulative"]), mib(cg["anon_peak_sampled"]), mib(cg["file_peak_sampled"]),
                     f"{r['fds']['before']}→{r['fds']['peak']}→{r['fds']['after_recovery']}",
                     f"{r['sockets']['established_before']}→{r['sockets']['established_peak']}→{r['sockets']['established_after_recovery']}",
                     f"{ev.get('oom', 0)}/{ev.get('oom_kill', 0)}/{ev.get('max', 0)}", r["gate"]))
    print(table(["case", "req/s", "GiB/s", "p50 ms", "p95 ms", "p99 ms", "CPU cores", "RSS peak MiB", "cgroup cur. peak MiB (sampled)",
                 "cgroup peak MiB (kernel, cumulative)", "anon peak MiB", "file peak MiB", "FDs before→peak→after", "TCP est. before→peak→after",
                 "Δ oom/oom_kill/max", "gate"], body))
    drain = next((r for r in data if r.get("case") == "idle-drain"), None)
    print()
    print(f"limit {meta['memory']} (memory.max {meta['memory_max_bytes']}), image {meta['image']} on {meta['platform']}, "
          f"{meta['worker_threads']} worker threads, {meta['duration_s']:.0f} s cases, {meta['soak_s']:.0f} s soak, concurrency {meta['concurrency']}")
    if drain:
        print(f"idle drain: {drain['gate']} after {drain['idle_s']} s idle; FDs {drain['fds']['baseline']}→{drain['fds']['after']}, "
              f"TCP {drain['sockets']['established_baseline']}→{drain['sockets']['established_after']}")


def suite(path):
    data = rows(path)
    meta = next(r for r in data if r["kind"] == "meta")
    print(f"{meta['machine']}, {meta['cpus']} CPUs, commit {meta['commit']}, load average at start {meta['load_before']}\n")
    print(table(["step", "seconds"], [(r["case"], r["seconds"]) for r in data if r["kind"] in ("build", "run")]))
    print()
    print(table(["test binary", "result", "passed", "failed", "ignored", "seconds"],
                [(r["binary"], r["result"], r["passed"], r["failed"], r["ignored"], r["seconds"]) for r in data if r["kind"] == "crate" and r["passed"] + r["failed"]]))
    print()
    print(table(["#", "test", "seconds"], [(i + 1, f"{r['binary']}: {r['test']}", r["seconds"]) for i, r in enumerate(x for x in data if x["kind"] == "slowest")]))


if __name__ == "__main__":
    {"micro": lambda: micro(sys.argv[2]), "compare": lambda: compare(sys.argv[2], sys.argv[3]),
     "http": lambda: http(sys.argv[2]), "suite": lambda: suite(sys.argv[2])}[sys.argv[1]]()
