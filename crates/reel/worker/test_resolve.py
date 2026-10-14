"""python3 -m unittest crates/reel/worker/test_resolve.py

yt-dlp is replaced by a stub module, so this needs neither the zipapp nor the network: what is held
still is the worker's own bookkeeping of its cached YoutubeDL instances.
"""

import importlib
import os
import sys
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class DownloadError(Exception):
    pass


class FakeYoutubeDL:
    built = 0
    closed = 0
    outcomes = []  # what each extract_info does, in order: an exception to raise, or an info dict

    def __init__(self, options):
        FakeYoutubeDL.built += 1
        self.options = options

    def extract_info(self, url, download=False):
        outcome = FakeYoutubeDL.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def close(self):
        FakeYoutubeDL.closed += 1


OK = {"url": "https://example/v", "width": 1, "height": 1, "formats": []}


def load():
    yt_dlp = types.ModuleType("yt_dlp")
    yt_dlp.YoutubeDL = FakeYoutubeDL
    utils = types.ModuleType("yt_dlp.utils")
    utils.DownloadError = DownloadError
    sys.modules["yt_dlp"], sys.modules["yt_dlp.utils"] = yt_dlp, utils
    sys.modules.pop("resolve", None)
    return importlib.import_module("resolve")


class Instances(unittest.TestCase):
    def setUp(self):
        FakeYoutubeDL.built = FakeYoutubeDL.closed = 0
        self.resolve = load()
        self.request = {"id": "dQw4w9WgXcQ", "format": "f", "cache": "/c"}

    def ask(self, *outcomes):
        FakeYoutubeDL.outcomes = list(outcomes)
        results = []
        for _ in outcomes:
            try:
                results.append(self.resolve.answer(self.request))
            except Exception as failure:  # noqa: BLE001 - the worker reports these, never raises
                results.append(failure)
        return results

    def test_one_instance_serves_many_requests(self):
        self.ask(OK, OK, OK)
        self.assertEqual(FakeYoutubeDL.built, 1)

    def test_a_failure_about_the_video_keeps_the_instance(self):
        self.ask(DownloadError("ERROR: [youtube] x: Video unavailable"), OK)
        self.assertEqual(FakeYoutubeDL.built, 1)

    def test_an_age_gate_is_about_the_video_too(self):
        self.ask(DownloadError("ERROR: [youtube] x: Sign in to confirm your age"), OK)
        self.assertEqual(FakeYoutubeDL.built, 1)

    def test_a_bot_check_drops_the_instance(self):
        for said in (
            "ERROR: [youtube] x: Sign in to confirm you're not a bot",
            "ERROR: [youtube] x: Sign in to confirm you’re not a bot",
            "ERROR: [youtube] x: HTTP Error 429: Too Many Requests",
            "ERROR: unable to download video data: HTTP Error 429",
        ):
            FakeYoutubeDL.built = FakeYoutubeDL.closed = 0
            self.resolve._instances.clear()
            self.ask(DownloadError(said), OK)
            self.assertEqual(FakeYoutubeDL.built, 2, said)
            self.assertEqual(FakeYoutubeDL.closed, 1, said)

    def test_an_unexpected_error_drops_the_instance(self):
        self.ask(RuntimeError("boom"), OK)
        self.assertEqual(FakeYoutubeDL.built, 2)

    def test_the_cache_is_bounded(self):
        for n in range(6):
            FakeYoutubeDL.outcomes = [OK]
            self.resolve.answer({"id": "dQw4w9WgXcQ", "format": f"f{n}"})
        self.assertEqual(len(self.resolve._instances), 4)

    def test_extractor_args_are_lists(self):
        parsed = self.resolve.extractor_args("youtube:player_client=a,b;player_skip=x")
        self.assertEqual(parsed, {"youtube": {"player_client": ["a", "b"], "player_skip": ["x"]}})


if __name__ == "__main__":
    unittest.main()
