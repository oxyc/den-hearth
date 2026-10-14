#!/usr/bin/env python3
"""Checks the harness without a container engine: the gate accepts a healthy case and REJECTS each failure it exists
to catch (OOM, a limit hit, leaked descriptors/sockets/processes, missing health, wrong statuses, client errors),
the load driver's framing checks reject malformed responses, and the divan/go parsers read their formats.

    python3 bench/http/test.py
"""
import http.server
import json
import pathlib
import subprocess
import sys
import threading
import unittest
import warnings

warnings.simplefilter("ignore", ResourceWarning)  # the throwaway test servers close their sockets at exit

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
import gate  # noqa: E402


def sample(**over):
    s = {
        "t": 0,
        "cgroup": {"current": 60 << 20, "peak": 70 << 20, "max": 512 << 20, "swap_max": 0, "swap_current": 0,
                   "events": {"low": 0, "high": 0, "max": 0, "oom": 0, "oom_kill": 0, "oom_group_kill": 0},
                   "anon": 12 << 20, "file": 40 << 20, "kernel": 2 << 20, "sock": 0, "shmem": 0, "file_dirty": 0, "pids": 6, "cpu_usec": 1_000_000},
        "init": {"pid": 7, "rss_kib": 20_000, "fds": 17, "threads": 5},
        "cgroup_processes": {"count": 1, "rss_kib": 20_000, "fds": 17, "comms": {"den-hearth": 1}},
        "tcp": {"states": {"established": 5}, "established": 5, "established_to_stub": 5},
    }
    for path, value in over.items():
        node = s
        keys = path.split(".")
        for key in keys[:-1]:
            node = node[key]
        node[keys[-1]] = value
    return s


RESULT = {"label": "x", "mode": "normal", "concurrency": 16, "elapsed_s": 10.0, "requests": 1000, "req_s": 100.0,
          "payload_bytes": 1 << 20, "gib_s": 0.0001, "p50_ms": 1.0, "p95_ms": 2.0, "p99_ms": 3.0, "statuses": {"200": 1000},
          "errors": 0, "error_outcomes": {}, "refusal_codes": {}}


def case(spec=None, result=None, before=None, after=None, recovered=None, baseline=None, healthy=True, stub=None):
    before = before or sample()
    after = after or sample(**{"cgroup.cpu_usec": 3_000_000})
    row, failures = gate.evaluate_case(
        {"name": "t", **(spec or {})}, {**RESULT, **(result or {})}, [before, after], before, after,
        recovered or sample(), baseline or sample(), healthy, *(stub or (None, None)))
    return row, failures


class Gate(unittest.TestCase):
    def test_a_healthy_case_passes_and_reports_cpu(self):
        row, failures = case()
        self.assertEqual(failures, [])
        self.assertEqual(row["gate"], "passed")
        self.assertAlmostEqual(row["cpu_cores"], 0.2)

    def rejects(self, text, **kw):
        _, failures = case(**kw)
        self.assertTrue(any(text in f for f in failures), failures)

    def test_oom_kill_in_the_case_fails(self):
        after = sample()
        after["cgroup"]["events"]["oom_kill"] = 1
        self.rejects("oom_kill", after=after)

    def test_hitting_the_limit_fails_unless_the_case_writes_files(self):
        after = sample()
        after["cgroup"]["events"]["max"] = 3
        self.rejects("max moved", after=after)
        _, failures = case(spec={"allow_max": True}, after=after)
        self.assertEqual(failures, [])

    def test_oom_fails_even_when_the_case_allows_max(self):
        after = sample()
        after["cgroup"]["events"]["oom"] = 1
        _, failures = case(spec={"allow_max": True}, after=after)
        self.assertTrue(failures)

    def test_leaked_descriptors_fail(self):
        self.rejects("file descriptors", recovered=sample(**{"cgroup_processes.fds": 60}))

    def test_a_pool_allowance_covers_idle_upstream_connections_and_no_more(self):
        leaky = sample(**{"cgroup_processes.fds": 60, "tcp.established": 48, "tcp.established_to_stub": 48})
        # against the run baseline the whole run's pool applies; against the case start only the case's
        _, failures = case(spec={"pool_allowance": 64}, recovered=leaky, baseline=sample())
        self.assertEqual(failures, [])
        _, failures = case(spec={"pool_allowance": 8}, recovered=leaky)
        self.assertTrue(failures)

    def test_leaked_established_sockets_fail(self):
        self.rejects("established sockets", recovered=sample(**{"tcp.established": 40}))

    def test_a_leftover_child_process_fails(self):
        self.rejects("cgroup processes", recovered=sample(**{"cgroup_processes.count": 3}))

    def test_memory_that_does_not_come_back_fails(self):
        self.rejects("unreclaimable", recovered=sample(**{"cgroup.anon": 200 << 20}))

    def test_a_cache_fill_allowance_admits_declared_growth_only(self):
        grown = sample(**{"cgroup.anon": 80 << 20})
        _, failures = case(spec={"cache_fill_bytes": 80 << 20}, recovered=grown, baseline=sample())
        self.assertEqual(failures, [])
        self.rejects("unreclaimable", recovered=grown)

    def test_page_cache_alone_is_not_a_leak(self):
        _, failures = case(recovered=sample(**{"cgroup.file": 400 << 20, "cgroup.current": 480 << 20}))
        self.assertEqual(failures, [])

    def test_missing_health_recovery_fails(self):
        self.rejects("/health", healthy=False)

    def test_unexpected_status_and_client_errors_fail(self):
        self.rejects("not allowed", result={"statuses": {"200": 900, "500": 100}})
        self.rejects("client errors", result={"errors": 3, "error_outcomes": {"OSError": 3}})
        self.rejects("refusal", result={"statuses": {"200": 1, "503": 999}, "refusal_codes": {"503:oops": 999}}, spec={"allowed_statuses": {200, 503}})

    def test_a_required_status_that_never_appeared_fails(self):
        self.rejects("never seen", result={"statuses": {"200": 1000}}, spec={"allowed_statuses": {200, 304}, "required_statuses": {200, 304}})

    def test_too_few_requests_fail(self):
        self.rejects("completed", result={"requests": 3}, spec={"min_requests": 100})

    def test_cancellation_expects_status_zero_and_upstream_evidence(self):
        _, failures = case(spec={"cancel": True}, result={"statuses": {"0": 1000}})
        self.assertEqual(failures, [])
        self.rejects("not allowed", spec={"cancel": True})
        stub = ({"slow": {"accepted": 0, "completed": 0, "aborted": 0}, "connections": 1}, {"slow": {"accepted": 0, "completed": 0, "aborted": 0}, "connections": 1})
        _, failures = case(spec={"cancel": True, "expect_upstream_aborts": True}, result={"statuses": {"0": 1000}}, stub=stub)
        self.assertTrue(any("never reached" in f for f in failures), failures)
        done = ({"slow": {"accepted": 0, "completed": 0, "aborted": 0}, "connections": 1}, {"slow": {"accepted": 10, "completed": 4, "aborted": 6}, "connections": 1})
        _, failures = case(spec={"cancel": True, "expect_upstream_aborts": True}, result={"statuses": {"0": 1000}}, stub=done)
        self.assertTrue(any("not aborted" in f for f in failures), failures)

    def test_the_initial_state_must_be_the_claimed_limit_with_no_swap_and_no_oom(self):
        gate.check_initial(sample(), 512 << 20)
        for bad in (sample(**{"cgroup.max": 1 << 30}), sample(**{"cgroup.swap_max": 1 << 30}),
                    sample(**{"cgroup.events.oom_kill": 2}), sample(**{"cgroup.events": {"max": 0}})):
            with self.assertRaises(gate.GateError):
                gate.check_initial(bad, 512 << 20)


class Driver(unittest.TestCase):
    """load.py against a server that frames responses wrongly: each must be counted as an error, not a pass."""

    @staticmethod
    def serve(raw):
        class H(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.wfile.write(raw)
                self.wfile.flush()
                self.close_connection = True

            def log_message(self, *_):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return server

    def run_load(self, raw, *extra):
        server = self.serve(raw)
        try:
            out = subprocess.run([sys.executable, str(HERE / "load.py"), "--base", f"http://127.0.0.1:{server.server_port}",
                                  "--path", "/", "--requests", "5", "--duration", "5", "--concurrency", "1", *extra],
                                 capture_output=True, text=True, timeout=60)
        finally:
            server.shutdown()
        self.assertEqual(out.returncode, 0, out.stderr)
        return json.loads(out.stdout)

    def test_a_good_response_passes(self):
        r = self.run_load(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok", "--expect-200-bytes", "2")
        self.assertEqual((r["errors"], r["statuses"]), (0, {"200": 5}))

    def test_a_truncated_body_is_an_error(self):
        r = self.run_load(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\nshort")
        self.assertEqual(r["errors"], 5, r)

    def test_a_wrong_size_is_an_error(self):
        r = self.run_load(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok", "--expect-200-bytes", "3")
        self.assertEqual(r["errors"], 5, r)

    def test_a_wrong_prefix_is_an_error(self):
        r = self.run_load(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nno", "--expect-prefix", "ok")
        self.assertEqual(r["errors"], 5, r)

    def test_a_refusal_carries_its_code(self):
        body = b'{"error":"rate_limited"}'
        r = self.run_load(b"HTTP/1.1 429 Too Many\r\nContent-Length: %d\r\nConnection: close\r\n\r\n" % len(body) + body)
        self.assertEqual(r["refusal_codes"], {"429:rate_limited": 5}, r)


class Parsers(unittest.TestCase):
    def test_divan_table_reads_times_allocations_and_throughput(self):
        def row(name, *cols):
            return name.ljust(26) + "".join(f"│ {c}".ljust(16) if i else c.ljust(14) for i, c in enumerate(cols)).rstrip()

        lines = [
            "Timer precision: 41 ns",
            "scout".ljust(26) + "fastest       │ slowest       │ median        │ mean          │ samples │ iters",
            row("├─ parse", "", "", "", ""),
            row("│  ╰─ small", "60.55 µs", "89.97 µs", "61.03 µs", "61.5 µs", "100", "100"),
            row("│", "176.1 MB/s", "118.5 MB/s", "174.7 MB/s", "173.4 MB/s"),
            row("│", "alloc:", "", "", ""),
            row("│", "  1283", "1283", "1283", "1283"),
            row("│", "  125.1 KB", "125.1 KB", "125.1 KB", "125.1 KB"),
            row("╰─ plain", "18.2 ns", "19.01 ns", "18.52 ns", "18.49 ns", "100", "25600"),
        ]
        table = "\n".join(lines) + "\n"
        out = subprocess.run([sys.executable, str(HERE.parent / "divan_to_jsonl.py"), "scout"], input=table, capture_output=True, text=True)
        rows = [json.loads(l) for l in out.stdout.splitlines()]
        self.assertEqual([r["bench"] for r in rows], ["parse/small", "plain"])
        self.assertEqual(rows[0]["median_ns"], 61030.0)
        self.assertEqual(rows[0]["alloc_count_median"], 1283.0)
        self.assertEqual(rows[0]["alloc_bytes_median"], 125100.0)
        self.assertEqual(rows[0]["throughput_median_bytes_s"], 174.7e6)
        self.assertEqual(rows[1]["median_ns"], 18.52)

    def test_go_output_is_reduced_to_median_and_fastest(self):
        text = ("BenchmarkRank/movie/100-10  \t 300\t 3000 ns/op\t 150 B/op\t 10 allocs/op\n"
                "BenchmarkRank/movie/100-10  \t 300\t 5000 ns/op\t 150 B/op\t 10 allocs/op\n"
                "BenchmarkRank/movie/100-10  \t 300\t 4000 ns/op\t 150 B/op\t 10 allocs/op\n")
        out = subprocess.run([sys.executable, str(HERE.parent / "go" / "go_to_jsonl.py")], input=text, capture_output=True, text=True)
        row = json.loads(out.stdout)
        self.assertEqual((row["bench"], row["median_ns"], row["fastest_ns"], row["alloc_count_median"]), ("Rank/movie/100", 4000.0, 3000.0, 10.0))


if __name__ == "__main__":
    unittest.main(warnings="ignore")
