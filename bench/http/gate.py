#!/usr/bin/env python3
"""The enforcing part of the HTTP harness: pure functions from measurements to a report row and a list of failures.

A case passes only if ALL of these hold; any failure makes the run exit non-zero and keeps its artifacts:

  traffic     no client error (reset, truncated body, Content-Length mismatch); every status is one the case allows,
              every required status was seen, every refusal carries an allowed `error` code; enough requests completed
  memory      memory.events `oom`, `oom_kill`, `oom_group_kill` did not move during the case; `max` (the cgroup hit
              its limit and had to reclaim) did not move either, unless the case writes files and says so
  recovery    after the load stops, within the recovery timeout: unreclaimable memory (anon + kernel + sock) is back
              within MEM_TOLERANCE of the case's start AND of the run's baseline, file descriptors within FD_TOLERANCE,
              established TCP sockets and cgroup processes back to the baseline, and /health answers
  cancellation (cancel cases) the server saw the cancelled requests: accepted > 0 upstream, or none expected

File pages (page cache) are reclaimable and rise after any disk I/O, so recovery is judged on anon + kernel + sock;
memory.current, the sampled peak and the kernel's memory.peak are all recorded in the row.
"""

import os

MEM_TOLERANCE = 8 * 1024 * 1024
FD_TOLERANCE = 2
RUN_POOL = 128  # idle pooled upstream connections a run may be carrying at any point; the final idle drain takes them to 0
# What the bounded caches may hold at any point in a run. Subtitles' memory tier is capped at 64 MiB (cache.rs
# MAX_MEMORY_BYTES) and Reel's resolve cache at 10,000 entries, but the process's anonymous memory after a
# unique-key run measured about 2x that (glibc keeps the freed 100 KB bodies in its arenas): the allowance is the
# measured high-water mark plus a margin, so growth beyond it is a regression and not a rediscovery of the allocator.
# BENCHMARKS.md records the measurement; a later PR that changes the allocator should lower this.
RUN_CACHE_FILL = int(os.environ.get("RUN_CACHE_FILL_MIB", "160")) * 1024 * 1024
FATAL_EVENTS = ("oom", "oom_kill", "oom_group_kill")
REQUIRED_EVENT_FIELDS = ("low", "high", "max", "oom", "oom_kill")


class GateError(Exception):
    pass


def unreclaimable(sample):
    cg = sample["cgroup"]
    return cg["anon"] + cg["kernel"] + cg["sock"]


def events_delta(before, after):
    keys = set(before["cgroup"]["events"]) | set(after["cgroup"]["events"])
    return {k: after["cgroup"]["events"].get(k, 0) - before["cgroup"]["events"].get(k, 0) for k in sorted(keys)}


def check_initial(sample, memory_max, require_zero_swap=True):
    """The container really has the limit the run claims, and has not already been through an OOM."""
    cg = sample["cgroup"]
    problems = []
    if cg.get("max") != memory_max:
        problems.append(f"memory.max={cg.get('max')} want {memory_max}")
    if require_zero_swap and cg.get("swap_max") not in (0,):
        problems.append(f"memory.swap.max={cg.get('swap_max')} want 0")
    missing = [f for f in REQUIRED_EVENT_FIELDS if f not in cg["events"]]
    if missing:
        problems.append(f"memory.events lacks {missing} (cgroup v2 required)")
    for field in FATAL_EVENTS:
        if cg["events"].get(field, 0):
            problems.append(f"initial memory.events {field}={cg['events'][field]}")
    if problems:
        raise GateError("initial state: " + "; ".join(problems))


def _peak(samples, getter, default=0):
    return max((getter(s) for s in samples), default=default)


def evaluate_case(spec, result, samples, before, after, recovered, baseline, health_ok, stub_before=None, stub_after=None):
    """Returns (row, failures). `spec` names what the case may and must see; see the module docstring."""
    failures = []
    name = spec["name"]
    statuses = {int(k): v for k, v in result["statuses"].items()}
    allowed = set(spec.get("allowed_statuses", {200}))
    required = set(spec.get("required_statuses", allowed if not spec.get("cancel") else set()))
    allowed_refusals = set(spec.get("allowed_refusals", ()))

    if spec.get("cancel"):
        # A cancelled request has no response: the driver reports status 0, and nothing else may appear.
        allowed = {0}
        required = {0}
    if result["errors"]:
        failures.append(f"{result['errors']} client errors: {result['error_outcomes']}")
    for status in statuses:
        if status not in allowed:
            failures.append(f"status {status} not allowed (allowed {sorted(allowed)}): {statuses}")
    for status in required:
        if not statuses.get(status):
            failures.append(f"required status {status} never seen: {statuses}")
    for code in result.get("refusal_codes", {}):
        if code not in allowed_refusals:
            failures.append(f"refusal {code} not allowed (allowed {sorted(allowed_refusals)})")
    if result["requests"] < spec.get("min_requests", 1):
        failures.append(f"only {result['requests']} requests completed, need {spec.get('min_requests', 1)}")
    exact = spec.get("exact_requests")
    if exact is not None and result["requests"] != exact:
        failures.append(f"completed {result['requests']} requests, the case requires exactly {exact}")

    delta = events_delta(before, after)
    for field in FATAL_EVENTS:
        if delta.get(field, 0):
            failures.append(f"memory.events {field} moved by {delta[field]} during the case")
    if delta.get("max", 0) and not spec.get("allow_max"):
        failures.append(f"memory.events max moved by {delta['max']}: the cgroup hit its limit")

    if not health_ok:
        failures.append("/health did not recover")

    # Idle keep-alive connections to an upstream stay pooled (reqwest keeps them 90 s), each holding a descriptor.
    # A case that talks to an upstream declares how many it may leave; the run's final idle-drain check proves they
    # all go.
    # Against the run baseline the allowance is the whole run's (RUN_POOL): an earlier upstream case's idle pool is
    # still there. Against the case's own start it is the case's: a case that does not talk to an upstream may not
    # add a connection.
    # Memory: the same split. A case that fills a bounded cache (unique keys) declares how much it may keep
    # (`cache_fill_bytes`); the whole run may keep RUN_CACHE_FILL.
    refs = (("case start", before, spec.get("pool_allowance", 0), spec.get("cache_fill_bytes", 0)),
            ("run baseline", baseline, RUN_POOL, RUN_CACHE_FILL))
    for label, ref, pool, fill in refs:
        grown = unreclaimable(recovered) - unreclaimable(ref)
        if grown > MEM_TOLERANCE + fill:
            failures.append(f"unreclaimable memory {grown} bytes above the {label} after recovery (anon+kernel+sock; allowed {MEM_TOLERANCE + fill})")
        if recovered["cgroup_processes"]["fds"] > ref["cgroup_processes"]["fds"] + FD_TOLERANCE + pool:
            failures.append(f"file descriptors {recovered['cgroup_processes']['fds']} vs {ref['cgroup_processes']['fds']} at the {label} (pool allowance {pool})")
        if recovered["tcp"]["established"] > ref["tcp"]["established"] + pool:
            failures.append(f"established sockets {recovered['tcp']['established']} vs {ref['tcp']['established']} at the {label} (pool allowance {pool})")
        if recovered["tcp"]["established_to_stub"] > ref["tcp"]["established_to_stub"] + pool:
            failures.append(f"upstream sockets {recovered['tcp']['established_to_stub']} vs {ref['tcp']['established_to_stub']} at the {label} (pool allowance {pool})")
        if recovered["cgroup_processes"]["count"] > ref["cgroup_processes"]["count"]:
            failures.append(f"cgroup processes {recovered['cgroup_processes']['comms']} vs {ref['cgroup_processes']['comms']} at the {label}")

    evidence = None
    if stub_before is not None and stub_after is not None:
        slow = {k: stub_after["slow"][k] - stub_before["slow"][k] for k in stub_after["slow"]}
        evidence = {"upstream_slow": slow, "stub_connections_after": stub_after["connections"]}
        if spec.get("expect_upstream_aborts"):
            if slow["accepted"] <= 0:
                failures.append("cancellation never reached the upstream (accepted=0): the case proved nothing")
            elif slow["completed"] != 0 or slow["aborted"] != slow["accepted"]:
                failures.append(f"upstream work was not aborted by the cancellation: {slow}")

    elapsed = result["elapsed_s"] or 1
    cg_a, cg_b = after["cgroup"], before["cgroup"]
    row = {
        "case": name,
        "gate": "failed" if failures else "passed",
        "failures": failures,
        "concurrency": result["concurrency"],
        "elapsed_s": result["elapsed_s"],
        "requests": result["requests"],
        "req_s": result["req_s"],
        "gib_s": result["gib_s"],
        "payload_bytes": result["payload_bytes"],
        "p50_ms": result["p50_ms"],
        "p95_ms": result["p95_ms"],
        "p99_ms": result["p99_ms"],
        "statuses": result["statuses"],
        "refusal_codes": result.get("refusal_codes", {}),
        "errors": result["errors"],
        "cpu_cores": round((cg_a["cpu_usec"] - cg_b["cpu_usec"]) / 1e6 / elapsed, 3),
        "rss_kib": {
            "before": before["cgroup_processes"]["rss_kib"],
            "peak": _peak(samples, lambda s: s["cgroup_processes"]["rss_kib"], before["cgroup_processes"]["rss_kib"]),
            "after_recovery": recovered["cgroup_processes"]["rss_kib"],
            "init_peak": _peak(samples, lambda s: s["init"]["rss_kib"], before["init"]["rss_kib"]),
        },
        "cgroup": {
            "limit": cg_a["max"],
            "current_before": cg_b["current"],
            "current_peak_sampled": _peak(samples, lambda s: s["cgroup"]["current"], cg_b["current"]),
            "current_after_recovery": recovered["cgroup"]["current"],
            "peak_kernel_cumulative": cg_a["peak"],
            "anon_peak_sampled": _peak(samples, lambda s: s["cgroup"]["anon"], cg_b["anon"]),
            "file_peak_sampled": _peak(samples, lambda s: s["cgroup"]["file"], cg_b["file"]),
            # What the kernel cannot reclaim (anon + kernel + sock), and the socket part of it: the number a
            # bounded server must hold down, and the one RSS never shows.
            "unreclaimable_peak_sampled": _peak(samples, unreclaimable, unreclaimable(before)),
            "sock_peak_sampled": _peak(samples, lambda s: s["cgroup"]["sock"], cg_b["sock"]),
            "events_delta": delta,
            "swap_current_after": cg_a["swap_current"],
        },
        "fds": {
            "before": before["cgroup_processes"]["fds"],
            "peak": _peak(samples, lambda s: s["cgroup_processes"]["fds"], 0),
            "after_recovery": recovered["cgroup_processes"]["fds"],
        },
        "sockets": {
            "established_before": before["tcp"]["established"],
            "established_peak": _peak(samples, lambda s: s["tcp"]["established"], 0),
            "established_after_recovery": recovered["tcp"]["established"],
            "to_stub_peak": _peak(samples, lambda s: s["tcp"]["established_to_stub"], 0),
            "to_stub_after_recovery": recovered["tcp"]["established_to_stub"],
        },
        "processes": {
            "before": before["cgroup_processes"]["count"],
            "peak": _peak(samples, lambda s: s["cgroup_processes"]["count"], 0),
            "after_recovery": recovered["cgroup_processes"]["count"],
        },
        "threads_peak": _peak(samples, lambda s: s["init"]["threads"], 0),
    }
    if evidence:
        row["upstream"] = evidence
    return row, failures


def recovered_enough(refs, recovered):
    """True when `recovered` is within tolerance of every (reference sample, pooled-connection allowance, cache-fill
    allowance in bytes): the orchestrator polls this."""
    for ref, pool, fill in refs:
        if unreclaimable(recovered) - unreclaimable(ref) > MEM_TOLERANCE + fill:
            return False
        if recovered["cgroup_processes"]["fds"] > ref["cgroup_processes"]["fds"] + FD_TOLERANCE + pool:
            return False
        if recovered["tcp"]["established"] > ref["tcp"]["established"] + pool:
            return False
        if recovered["tcp"]["established_to_stub"] > ref["tcp"]["established_to_stub"] + pool:
            return False
        if recovered["cgroup_processes"]["count"] > ref["cgroup_processes"]["count"]:
            return False
    return True
