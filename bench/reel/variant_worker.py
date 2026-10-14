"""The shipped worker with one knob switched off, so each knob's contribution can be measured alone.

`BENCH_FRESH=1` builds a new YoutubeDL per request (the pre-change behaviour). Everything else - the
protocol, the extractor-argument parsing, the error handling - is the shipped `resolve.py`, imported
unchanged.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "crates", "reel", "worker"))

import resolve  # noqa: E402

if os.environ.get("BENCH_FRESH") == "1":
    original = resolve.answer

    def fresh(request):
        for ydl in resolve._instances.values():
            ydl.close()
        resolve._instances.clear()
        return original(request)

    resolve.answer = fresh

resolve.main()
