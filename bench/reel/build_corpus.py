"""Freeze a trailer corpus: one flat YouTube search per title, sequential and 3 s apart.

TMDB's /videos is what Reel itself reads, but the benchmark must run without a key and be repeatable,
so the ids are searched once and committed as corpus.json. Re-run only to refresh a dead corpus.

    YTDLP_ZIP=/path/to/yt-dlp python3 -I bench/reel/build_corpus.py > bench/reel/corpus.json
"""

import json
import os
import subprocess
import sys
import time

TITLES = [
    "Dune Part Two", "Oppenheimer", "Barbie", "Inception", "Interstellar", "The Batman",
    "Top Gun Maverick", "Everything Everywhere All at Once", "Parasite", "Mad Max Fury Road",
    "Blade Runner 2049", "John Wick Chapter 4", "Spider-Man Across the Spider-Verse",
    "The Grand Budapest Hotel", "Get Out", "Knives Out", "Arrival", "Joker", "Poor Things",
    "Past Lives", "Anatomy of a Fall", "The Holdovers", "Challengers", "Civil War A24",
    "Furiosa", "Alien Romulus", "Deadpool and Wolverine", "Inside Out 2", "Wicked", "Gladiator II",
]
PER_TITLE = 2
TARGET = 44


def main():
    zipapp = os.environ["YTDLP_ZIP"]
    seen, out = set(), []
    for title in TITLES:
        if len(out) >= TARGET:
            break
        run = subprocess.run(
            [sys.executable, "-I", zipapp, "-q", "--no-warnings", "--flat-playlist", "--print",
             "%(id)s\t%(title)s\t%(duration)s", f"ytsearch6:{title} official trailer"],
            capture_output=True, text=True, timeout=60,
        )
        if run.returncode:
            sys.exit(f"search failed for {title!r}: {run.stderr[-300:]}")
        taken = 0
        for line in run.stdout.splitlines():
            vid, name, dur = (line.split("\t") + ["", ""])[:3]
            try:
                secs = float(dur)
            except ValueError:
                continue
            # A real trailer: named so, and long enough to be past the first minute.
            if "trailer" not in name.lower() or secs < 75 or vid in seen:
                continue
            seen.add(vid)
            out.append({"id": vid, "title": name, "duration": secs, "query": title})
            taken += 1
            if taken >= PER_TITLE:
                break
        time.sleep(3)
    json.dump(out, sys.stdout, indent=1)


main()
