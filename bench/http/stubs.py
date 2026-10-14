#!/usr/bin/env python3
"""Local stand-ins for every upstream den-hearth talks to, so the load harness makes no external call.

    stubs.py --port 9090 --testdata /testdata

One threaded HTTP/1.1 server, routed by prefix:

  /tmdb/3/...         TMDB (find, movie|tv/<n>/videos)                  -> HEARTH_STUB_TMDB
  /kinocheck/...      KinoCheck (movies|shows?imdb_id=)                 -> HEARTH_STUB_KINOCHECK
  /os/...             OpenSubtitles (GET /subtitles, POST /download)    -> HEARTH_STUB_OPENSUBTITLES
  /cdn/<id>.srt       the subtitle CDN the download link names (~100 KB; /cdn/slow-<id>.srt trickles)
  /<cfg>/stream/...   den-scout's stream list, as the Remux tests' fake origin answers it
  /p/<name>           302 -> /f/<name> (a debrid is always another origin)
  /f/<name>           the sample file from --testdata, with Range
  /slow?bytes=&chunk=&delay_ms=   a body that trickles; counted by /stats for cancellation evidence
  /stats              request counters per route, live connections, slow accepted/completed/aborted

No route reads a credential or reaches outside this process.
"""
import argparse
import hashlib
import json
import os
import re
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import multiprocessing

# Counters live in shared memory so /stats is global across the worker processes (SO_REUSEPORT spreads connections;
# one Python process tops out near a few thousand requests a second, which would make the stub the bottleneck).
NAMES = ["tmdb_find", "tmdb_videos", "kinocheck", "os_search", "os_download", "cdn", "cdn_slow", "scout_list", "play",
         "file", "slow", "connections", "slow_accepted", "slow_completed", "slow_aborted", "flag_slow_cdn"]
FORK = multiprocessing.get_context("fork")  # macOS defaults to spawn, which would not share the counters
SHARED = FORK.Array("q", len(NAMES))
ARGS = None


class _Gauge:
    """`LOCK`-compatible shim: the shared array carries its own lock."""

    def __enter__(self):
        SHARED.acquire()

    def __exit__(self, *_):
        SHARED.release()


LOCK = _Gauge()


def count(name, n=1):
    with LOCK:
        SHARED[NAMES.index(name)] += n


def youtube_id(n):
    """11 characters from [A-Za-z0-9_-], stable per title number."""
    return hashlib.sha1(f"yt-{n}".encode()).hexdigest()[:11]


def srt(file_id, cues=1800):
    out = []
    ms = 5000
    for i in range(1, cues + 1):
        end = ms + 1500 + (i % 7) * 300
        stamp = lambda t: f"{t // 3600000:02}:{t // 60000 % 60:02}:{t // 1000 % 60:02},{t % 1000:03}"
        out.append(f"{i}\n{stamp(ms)} --> {stamp(end)}\nLine {i} of subtitle {file_id}: a sentence a person reads.\n- Second line.\n\n")
        ms = end + 800 + (i % 11) * 200
    return "".join(out).encode()


def search_data(imdb_num, languages):
    data = []
    n = int(imdb_num or 0)
    for lang in (languages or "en").split(","):
        for k in range(6):
            fid = n * 100 + k * 7 + sum(map(ord, lang)) % 5
            data.append({
                "id": str(fid),
                "type": "subtitle",
                "attributes": {
                    "language": lang,
                    "download_count": 1000 - k * 90,
                    "hd": k % 2 == 0,
                    "fps": 23.976,
                    "release": f"Film.2024.{['1080p', '2160p', '720p'][k % 3]}.WEB-DL.DDP5.1.H.264-GRP{k}",
                    "from_trusted": k == 0,
                    "ratings": 7.5 - k * 0.5,
                    "moviehash_match": False,
                    "machine_translated": False,
                    "ai_translated": False,
                    "foreign_parts_only": False,
                    "hearing_impaired": k == 5,
                    "files": [{"file_id": fid, "file_name": f"{fid}.srt"}],
                },
            })
    return {"total_pages": 1, "total_count": len(data), "per_page": 60, "page": 1, "data": data}


def read_file(name):
    path = os.path.join(ARGS.testdata, os.path.basename(name))
    with open(path, "rb") as f:
        return f.read()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "stub"

    def log_message(self, *_):
        pass

    def setup(self):
        super().setup()
        # http.server writes the head and the body separately: with Nagle on, the second write waits for the
        # client's delayed ACK (~40 ms) and the stub, not the service, sets the latency.
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        count("connections")

    def finish(self):
        try:
            super().finish()
        finally:
            count("connections", -1)

    def send(self, status, body=b"", ctype="application/json", extra=()):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in extra:
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def json(self, status, value):
        self.send(status, json.dumps(value, separators=(",", ":")).encode())

    def do_HEAD(self):
        self.do_GET()

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        url = urlsplit(self.path)
        if url.path == "/os/download":
            count("os_download")
            try:
                fid = json.loads(body).get("file_id", 0)
            except ValueError:
                return self.json(400, {"message": "bad body"})
            host = self.headers.get("Host", "stub:9090")
            with LOCK:
                slow = ARGS.slow_cdn or SHARED[NAMES.index("flag_slow_cdn")]
            kind = "slow-" if slow else ""
            return self.json(200, {"link": f"http://{host}/cdn/{kind}{fid}.srt", "file_name": f"{fid}.srt", "requests": 1, "remaining": 1000})
        if url.path == "/control/slow_cdn":
            on = parse_qs(url.query).get("on", ["0"])[0] == "1"
            with LOCK:
                SHARED[NAMES.index("flag_slow_cdn")] = 1 if on else 0
            return self.json(200, {"slow_cdn": on})
        self.json(404, {"error": "not_found"})

    def do_GET(self):
        url = urlsplit(self.path)
        path, query = url.path, parse_qs(url.query)
        if path == "/stats":
            with LOCK:
                snapshot = dict(zip(NAMES, SHARED[:]))
            slow = {k: snapshot.pop(f"slow_{k}") for k in ("accepted", "completed", "aborted")}
            snapshot.pop("flag_slow_cdn")
            return self.json(200, {"counters": snapshot, "connections": snapshot.pop("connections"), "slow": slow})
        if path == "/health":
            return self.json(200, {"status": "ok"})

        m = re.fullmatch(r"/tmdb/3/find/(tt\d+)", path)
        if m:
            count("tmdb_find")
            n = int(m.group(1)[2:])
            return self.json(200, {"movie_results": [{"id": n, "title": f"Film {n}", "release_date": "2024-03-01"}], "tv_results": [{"id": n, "name": f"Show {n}"}]})
        m = re.fullmatch(r"/tmdb/3/(movie|tv)/(\d+)/videos", path)
        if m:
            count("tmdb_videos")
            n = int(m.group(2))
            return self.json(200, {"id": n, "results": [
                {"iso_639_1": "en", "name": "Official Trailer", "key": youtube_id(n), "site": "YouTube", "type": "Trailer", "official": True, "published_at": "2024-01-01T00:00:00.000Z"},
                {"iso_639_1": "en", "name": "Teaser", "key": youtube_id(n + 1), "site": "YouTube", "type": "Teaser", "official": True, "published_at": "2023-12-01T00:00:00.000Z"},
                {"iso_639_1": "en", "name": "Clip", "key": youtube_id(n + 2), "site": "YouTube", "type": "Clip", "official": False, "published_at": "2023-11-01T00:00:00.000Z"},
            ]})
        if path in ("/kinocheck/movies", "/kinocheck/shows"):
            count("kinocheck")
            n = int((query.get("imdb_id", ["tt0"])[0] or "tt0")[2:] or 0)
            return self.json(200, {"trailer": {"youtube_video_id": youtube_id(n + 100)}})
        if path == "/os/subtitles":
            count("os_search")
            return self.json(200, search_data(query.get("imdb_id", ["0"])[0], query.get("languages", ["en"])[0]))
        m = re.fullmatch(r"/cdn/(slow-)?(\d+)\.srt", path)
        if m:
            count("cdn_slow" if m.group(1) else "cdn")
            body = srt(m.group(2))
            if not m.group(1):
                return self.send(200, body, "application/x-subrip")
            return self.trickle(body, 16384, 5, "application/x-subrip", counted=True)

        m = re.fullmatch(r"/([^/]+)/stream/(movie|series)/([^/]+)\.json", path)
        if m:
            count("scout_list")
            host = self.headers.get("Host", "stub:9090")
            name = ARGS.sample
            return self.json(200, {"streams": [
                {"title": "decoy.mkv", "url": f"http://{host}/p/missing.mkv", "attributes": {"codec": "h264", "cached": False, "label": "uncached"}, "behaviorHints": {"filename": "decoy.mkv"}},
                {"title": name, "url": f"http://{host}/p/{name}", "attributes": {"cached": True, "label": f"fixture {name}"}, "behaviorHints": {"filename": name}},
            ]})
        if path.startswith("/p/"):
            count("play")
            host = self.headers.get("Host", "stub:9090")
            return self.send(302, b"", "text/plain", [("Location", f"http://{host}/f/{path[3:]}")])
        if path.startswith("/f/"):
            count("file")
            try:
                data = read_file(path[3:])
            except OSError:
                return self.send(404, b"")
            size = len(data)
            rng = self.headers.get("Range")
            if rng and rng.startswith("bytes="):
                a, _, b = rng[6:].partition("-")
                a = int(a)
                if a >= size:
                    return self.send(416, b"")
                b = min(int(b), size - 1) if b else size - 1
                chunk = data[a:b + 1]
                return self.send(206, chunk, "video/x-matroska", [("Accept-Ranges", "bytes"), ("Content-Range", f"bytes {a}-{b}/{size}")])
            return self.send(200, data, "video/x-matroska", [("Accept-Ranges", "bytes")])

        if path == "/slow":
            count("slow")
            total = int(query.get("bytes", ["262144"])[0])
            chunk = int(query.get("chunk", ["16384"])[0])
            delay = int(query.get("delay_ms", ["5"])[0])
            return self.trickle(b"x" * total, chunk, delay, "application/octet-stream", counted=True)
        self.json(404, {"error": "not_found"})

    def trickle(self, body, chunk, delay_ms, ctype, counted=False):
        if counted:
            count("slow_accepted")
        try:
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            for i in range(0, len(body), chunk):
                self.wfile.write(body[i:i + chunk])
                self.wfile.flush()
                time.sleep(delay_ms / 1000)
        except (BrokenPipeError, ConnectionResetError):
            if counted:
                count("slow_aborted")
            self.close_connection = True
            return
        if counted:
            count("slow_completed")


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 512
    allow_reuse_address = True
    allow_reuse_port = True  # N worker processes share the port (Python 3.11+)


def main():
    global ARGS
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=9090)
    p.add_argument("--testdata", default="/testdata")
    p.add_argument("--sample", default="h264.mkv", help="the testdata file the scout stub lists")
    p.add_argument("--slow-cdn", action="store_true", help="download links point at the trickling CDN")
    p.add_argument("--workers", type=int, default=4)
    ARGS = p.parse_args()

    def serve():
        Server(("0.0.0.0", ARGS.port), Handler).serve_forever()

    workers = [FORK.Process(target=serve, daemon=True) for _ in range(ARGS.workers)]
    for w in workers:
        w.start()
    print(f"stubs listening on :{ARGS.port} ({ARGS.workers} workers)", flush=True)
    for w in workers:
        w.join()


if __name__ == "__main__":
    main()
