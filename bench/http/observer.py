#!/usr/bin/env python3
"""Reads one container's resource use from OUTSIDE its cgroup, so the measurement is never charged to it.

Runs in a sidecar started with --pid=host --cgroupns=host --privileged and /sys/fs/cgroup visible. The target is
the host-VM pid of the container's init (`docker inspect -f '{{.State.Pid}}'`).

    observer.py --once   <pid>                 one JSON sample on stdout
    observer.py --sample <pid> [interval_s]    one JSON sample per line until killed

A sample holds: the cgroup's memory.current / peak / max / swap, memory.events (low high max oom oom_kill ...), the
anon / file / kernel / sock split of memory.stat, cpu.stat usage, pids; and for the container's init and every
process in the cgroup: RSS, file descriptors, and TCP sockets by state (read from the init's network namespace),
plus how many of those are established to the stub port 9090.
"""
import json
import os
import sys
import time

CGROUP_ROOT = "/sys/fs/cgroup"
TCP_STATES = {"01": "established", "02": "syn_sent", "03": "syn_recv", "04": "fin_wait1", "05": "fin_wait2",
              "06": "time_wait", "07": "close", "08": "close_wait", "09": "last_ack", "0A": "listen", "0B": "closing"}
STUB_PORT_HEX = "%04X" % 9090


def read(path, default=None):
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return default


def flat_keyed(text):
    out = {}
    for line in (text or "").splitlines():
        parts = line.split()
        if len(parts) == 2:
            try:
                out[parts[0]] = int(parts[1])
            except ValueError:
                pass
    return out


def cgroup_dir(pid):
    line = (read(f"/proc/{pid}/cgroup") or "").strip().splitlines()
    if not line:
        raise SystemExit(f"pid {pid} has no cgroup")
    return CGROUP_ROOT + line[0].split("::", 1)[1]


def int_file(path):
    text = (read(path) or "").strip()
    return None if text in ("", "max") else int(text)


def vm_rss_kib(pid):
    for line in (read(f"/proc/{pid}/status") or "").splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    return 0


def sockets(pid):
    counts, upstream = {}, 0
    for table in ("tcp", "tcp6"):
        for line in (read(f"/proc/{pid}/net/{table}") or "").splitlines()[1:]:
            fields = line.split()
            if len(fields) < 4:
                continue
            state = TCP_STATES.get(fields[3], fields[3])
            counts[state] = counts.get(state, 0) + 1
            if state == "established" and fields[2].rsplit(":", 1)[-1] == STUB_PORT_HEX:
                upstream += 1
    return counts, upstream


def fd_count(pid):
    try:
        return len(os.listdir(f"/proc/{pid}/fd"))
    except OSError:
        return 0


def sample(pid, cg):
    stat = flat_keyed(read(f"{cg}/memory.stat"))
    procs = [int(p) for p in (read(f"{cg}/cgroup.procs") or "").split()]
    comms = {}
    rss_total = fds_total = 0
    for p in procs:
        rss_total += vm_rss_kib(p)
        fds_total += fd_count(p)
        comm = (read(f"/proc/{p}/comm") or "?").strip()
        comms[comm] = comms.get(comm, 0) + 1
    socks, upstream = sockets(pid)
    return {
        "t": round(time.time(), 3),
        "cgroup": {
            "current": int_file(f"{cg}/memory.current"),
            "peak": int_file(f"{cg}/memory.peak"),
            "max": int_file(f"{cg}/memory.max"),
            "swap_max": int_file(f"{cg}/memory.swap.max"),
            "swap_current": int_file(f"{cg}/memory.swap.current"),
            "events": flat_keyed(read(f"{cg}/memory.events")),
            "anon": stat.get("anon", 0),
            "file": stat.get("file", 0),
            "kernel": stat.get("kernel", 0),
            "sock": stat.get("sock", 0),
            "shmem": stat.get("shmem", 0),
            "file_dirty": stat.get("file_dirty", 0),
            "pids": int_file(f"{cg}/pids.current"),
            "cpu_usec": flat_keyed(read(f"{cg}/cpu.stat")).get("usage_usec", 0),
        },
        "init": {"pid": pid, "rss_kib": vm_rss_kib(pid), "fds": fd_count(pid),
                 "threads": int((read(f"/proc/{pid}/status") or "Threads: 0").split("Threads:")[-1].split()[0])},
        "cgroup_processes": {"count": len(procs), "rss_kib": rss_total, "fds": fds_total, "comms": comms},
        "tcp": {"states": socks, "established": socks.get("established", 0), "established_to_stub": upstream},
    }


def main():
    mode, pid = sys.argv[1], int(sys.argv[2])
    cg = cgroup_dir(pid)
    if mode == "--once":
        print(json.dumps(sample(pid, cg), separators=(",", ":")))
        return
    interval = float(sys.argv[3]) if len(sys.argv) > 3 else 0.25
    while True:
        started = time.time()
        print(json.dumps(sample(pid, cg), separators=(",", ":")), flush=True)
        time.sleep(max(0.0, interval - (time.time() - started)))


if __name__ == "__main__":
    main()
