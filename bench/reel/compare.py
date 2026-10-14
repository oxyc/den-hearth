"""Does the youtubei.js resolver answer the same question yt-dlp does?

Both shipped workers are asked about the same videos at the same rungs, back to back, and what they
chose is compared: the itags (format set), heights, whether an HLS master came back and how its
variants read, and whether each answer plays past 60 s. Output is JSONL (one row per video and cap)
plus a summary of every difference.

    YTDLP_ZIP=... DENO_BIN=... DENO_DIR=... python3 bench/reel/compare.py --out compare.jsonl

Sequential with `GAP_SECS` between resolves, like bench.py.
"""

import argparse
import collections
import json
import os
import re
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import bench  # noqa: E402

REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
YT_DIR = os.path.join(REPO, "crates", "reel", "worker", "youtubei")

# What an itag is, for the ones the avc1 + mp4a ladder can pick. Codec families only: the question is
# whether the two resolvers chose the same kind of stream, not what YouTube calls it.
ITAGS = {
    137: "avc1 1080", 136: "avc1 720", 135: "avc1 480", 134: "avc1 360", 133: "avc1 240",
    299: "avc1 1080 60fps", 298: "avc1 720 60fps", 140: "mp4a 128k", 139: "mp4a 48k", 18: "avc1+mp4a 360 muxed",
}


def itag(url):
    value = bench.param(url, "itag")
    return int(value) if value and value.isdigit() else None


def master(url):
    """The HLS master's variant lines, reduced to (resolution, codecs): what a player chooses between."""
    if not url:
        return None
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": bench.UA}), timeout=15) as r:
            text = r.read().decode()
    except Exception as e:  # noqa: BLE001
        return f"fetch failed: {type(e).__name__}"
    out = []
    for line in text.splitlines():
        if line.startswith("#EXT-X-STREAM-INF"):
            attrs = dict(re.findall(r'([A-Z0-9-]+)=("[^"]*"|[^,]*)', line.split(":", 1)[1]))
            out.append((attrs.get("RESOLUTION"), attrs.get("CODECS", "").strip('"')))
    return sorted(set(out), key=str)


def run(args):
    with open(os.path.join(HERE, "corpus.json")) as f:
        corpus = json.load(f)[: args.videos]
    env = {"YTDLP_ZIP": os.environ["YTDLP_ZIP"]}
    ytdlp = bench.Worker(os.path.join(REPO, "crates", "reel", "worker", "resolve.py"), env)
    deno = bench.Worker(
        os.path.join(YT_DIR, "main.mjs"),
        {"DENO_DIR": os.environ["DENO_DIR"]},
        cmd=[os.environ["DENO_BIN"], "run", "--quiet", "--no-prompt", "--cached-only", "--frozen",
             f"--config={YT_DIR}/deno.json", f"--lock={YT_DIR}/deno.lock",
             "--allow-net=www.youtube.com,youtubei.googleapis.com", "{script}"],
    )
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    cache = os.path.join(os.environ.get("BENCH_SCRATCH", "/tmp"), "compare-cache")
    if args.ids:
        wanted = args.ids.split(",")
        jobs = [(v, 1080) for v in corpus if v["id"] in wanted]
    else:
        jobs = [(v, 1080) for v in corpus] + [(v, cap) for v in corpus[: args.capped] for cap in (720, 480)]
    for video, cap in jobs:
        rungs = [cap] + [s for s in (720, 480) if s < cap]
        rows = {}
        for name, worker, spec in (("ytdlp", ytdlp, {"format": bench.ladder(cap), "cache": cache}),
                                   ("youtubei", deno, {})):
            started = time.monotonic()
            reply = worker.ask({"id": video["id"], "rungs": rungs, **spec})
            ms = round((time.monotonic() - started) * 1000)
            row = {"ok": bool(reply.get("ok")), "ms": ms}
            if reply.get("ok"):
                row.update(
                    itags=[itag(u) for u in reply["urls"]], height=reply.get("height"), width=reply.get("width"),
                    # Which audio track: the language/content tags in the stream's own URL.
                    audio=bench.param(reply["urls"][1], "xtags") if len(reply["urls"]) > 1 else None,
                    hls=bool(reply.get("hls")), variants=master(reply.get("hls")),
                    playable=bench.check(reply, video.get("duration"))["playable"],
                )
            else:
                row["error"] = (reply.get("error") or "")[-200:]
            rows[name] = row
            time.sleep(bench.GAP_SECS)
        line = {"vid": video["id"], "cap": cap, **rows}
        with open(args.out, "a") as f:
            f.write(json.dumps(line) + "\n")
        print(json.dumps({"vid": video["id"], "cap": cap, "yt": rows["ytdlp"].get("itags"), "js": rows["youtubei"].get("itags")}), flush=True)
    ytdlp.stop()
    deno.stop()


def summarize(path):
    rows = [json.loads(line) for line in open(path)]
    diff = collections.Counter()
    notes = []
    for r in rows:
        a, b = r["ytdlp"], r["youtubei"]
        if not (a["ok"] and b["ok"]):
            diff["one side failed" if a["ok"] != b["ok"] else "both failed"] += 1
            if a["ok"] != b["ok"]:
                notes.append((r["vid"], r["cap"], "ytdlp ok" if a["ok"] else "youtubei ok", (a.get("error") or b.get("error"))))
            continue
        diff["compared"] += 1
        for key in ("itags", "height", "width", "hls", "audio"):
            if a[key] != b[key]:
                diff[f"{key} differ"] += 1
                notes.append((r["vid"], r["cap"], key, f"ytdlp={a[key]} youtubei={b[key]}"))
        if a["variants"] != b["variants"]:
            diff["hls variants differ"] += 1
            notes.append((r["vid"], r["cap"], "variants", f"ytdlp={a['variants']} youtubei={b['variants']}"))
        diff["ytdlp playable"] += a["playable"]
        diff["youtubei playable"] += b["playable"]
    for key, n in diff.items():
        print(f"{key}: {n}")
    for note in notes[:60]:
        print(note)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--videos", type=int, default=44)
    ap.add_argument("--capped", type=int, default=10, help="videos also asked at the 720 and 480 rungs")
    ap.add_argument("--ids", help="comma-separated video ids to compare at the full ladder, instead of the corpus")
    ap.add_argument("--summarize", action="store_true")
    a = ap.parse_args()
    summarize(a.out) if a.summarize else run(a)
