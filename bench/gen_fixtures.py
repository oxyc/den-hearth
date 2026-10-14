#!/usr/bin/env python3
"""Deterministic benchmark inputs, shared by the Rust (divan) and Go benchmarks so both measure the same bytes.

    bench/gen_fixtures.py [OUT_DIR]        # default: bench/fixtures (gitignored)

Writes:
  releases.json         1 object per release: title, size (bytes|null), seeders (int|null), cached, hash
  streams-500.json      a Stremio /stream body carrying those releases (the shape torrentio / comet answer with)
  streams-50.json       the same, first 50 releases
  torbox-listing.json   a TorBox /torrents/mylist body, ~10 MiB: each entry carries the id and hash scout keeps
                        and the file list scout skips
  config.json           an install config of the size /configure issues (two debrid accounts, four indexers)

Standard library only, seeded: the same bytes on every machine.
"""
import base64
import hashlib
import json
import pathlib
import random
import sys

OUT = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else pathlib.Path(__file__).parent / "fixtures")
OUT.mkdir(parents=True, exist_ok=True)
rng = random.Random(282)

FILMS = [
    ("Dune Part Two", 2024), ("The Batman", 2022), ("Oppenheimer", 2023), ("Blade Runner 2049", 2017),
    ("The Lord of the Rings The Fellowship of the Ring", 2001), ("Spider-Man Across the Spider-Verse", 2023),
    ("Mad Max Fury Road", 2015), ("Inception", 2010), ("Parasite", 2019), ("Pokemon The First Movie", 1998),
]
RES = ["2160p", "2160p", "1080p", "1080p", "1080p", "720p", "480p", "4K", "UHD", ""]
SRC = ["BluRay", "BluRay", "REMUX", "WEB-DL", "WEB-DL", "WEBRip", "HDTV", "BDRip", "DVDRip", "WEB", "AMZN.WEB-DL", "NF.WEB-DL"]
CODEC = ["x264", "x265", "H.264", "H.265", "HEVC", "AV1", "XviD", "10bit.HEVC"]
AUDIO = ["DDP5.1", "DDP5.1.Atmos", "AAC", "AC3", "DTS", "DTS-HD.MA.7.1", "TrueHD.7.1.Atmos", "FLAC", "DD+", "5.1", ""]
HDR = ["", "", "", "HDR", "HDR10+", "DV", "DV.HDR10", "HLG", "SDR"]
GROUP = ["FLUX", "NTb", "FraMeSToR", "TEPES", "RARBG", "YTS.MX", "SPARKS", "GalaxyRG", "EVO", "ION10", "CMRG", "Tigole", "QxR", "MeGusta"]
JUNK = ["HDCAM", "CAMRip", "HD-TS", "TELESYNC", "HDTC", "SCR", "DVDScr", "R5", "1xBET", "AI.Upscaled", "Workprint"]
SHOWS = ["Pokemon", "Pokemon Horizons", "Pokemon Master Journeys", "Severance", "The Bear"]
SIZE_RE_UNITS = [("GB", 1 << 30), ("GiB", 1 << 30), ("MB", 1 << 20)]


def film_release():
    title, year = rng.choice(FILMS)
    sep = rng.choice([".", " ", "."])
    parts = [title.replace(" ", sep), str(year)]
    if rng.random() < 0.06:
        parts.append(rng.choice(JUNK))
    else:
        parts += [p for p in (rng.choice(RES), rng.choice(SRC), rng.choice(HDR), rng.choice(AUDIO), rng.choice(CODEC)) if p]
    return sep.join(parts) + "-" + rng.choice(GROUP) + rng.choice([".mkv", ".mp4", "", ""])


def episode_release():
    show = rng.choice(SHOWS)
    s, e = rng.choice([(1, 1), (1, 5), (2, 3), (0, 82)])
    sep = rng.choice([".", " "])
    kind = rng.random()
    if kind < 0.15:
        label = f"S{s:02d}.COMPLETE"
    elif kind < 0.25:
        label = f"Season {s}"
    else:
        label = f"S{s:02d}E{e:02d}"
    parts = [show.replace(" ", sep), label, rng.choice(RES), rng.choice(SRC), rng.choice(AUDIO), rng.choice(CODEC)]
    return sep.join(p for p in parts if p) + "-" + rng.choice(GROUP)


def releases(n):
    out = []
    for i in range(n):
        title = film_release() if rng.random() < 0.55 else episode_release()
        gib = rng.choice([0.7, 1.4, 2.3, 4.5, 8.0, 15.2, 24.0, 41.0, 68.0])
        size = int(gib * (1 << 30) * rng.uniform(0.8, 1.2)) if rng.random() < 0.9 else None
        seeders = int(rng.paretovariate(1.1) * 3) if rng.random() < 0.85 else None
        out.append({
            "title": title,
            "size": size,
            "seeders": seeders,
            "cached": rng.random() < 0.3,
            "hash": hashlib.sha1(f"release-{i}".encode()).hexdigest(),
        })
    return out


def stremio(rel):
    streams = []
    for i, r in enumerate(rel):
        gb = (r["size"] or 0) / (1 << 30)
        desc = r["title"] + "\n"
        if r["seeders"] is not None:
            desc += f"\U0001F464 {r['seeders']} "
        if r["size"]:
            desc += f"\U0001F4BE {gb:.2f} GB "
        desc += "⚙️ " + rng.choice(["ThePirateBay", "YTS", "1337x", "EZTV", "RARBG"])
        s = {"name": "Torrentio\n" + rng.choice(["4k", "1080p", "720p"]), "title": desc, "infoHash": r["hash"].upper() if i % 7 == 0 else r["hash"]}
        if i % 3 == 0:
            s["fileIdx"] = rng.randrange(0, 40)
        if i % 5 == 0:
            s["behaviorHints"] = {"bingeGroup": "torrentio|" + rng.choice(["4k", "1080p"]), "filename": r["title"] + ".mkv"}
            if r["size"]:
                s["behaviorHints"]["videoSize"] = r["size"]
        streams.append(s)
    return {"streams": streams}


def torbox_listing(target_bytes):
    entries = []
    total = 0
    i = 0
    while total < target_bytes:
        i += 1
        h = hashlib.sha1(f"torbox-{i}".encode()).hexdigest()
        files = [
            {"id": j, "md5": None, "hash": h, "name": f"Show.S01E{j + 1:02d}.1080p.WEB-DL.DDP5.1.H.264-GROUP/{j}.mkv",
             "size": rng.randrange(500_000_000, 4_000_000_000), "s3_path": f"{h}/{j}.mkv",
             "mimetype": "video/x-matroska", "short_name": f"{j}.mkv", "absolute_path": f"/{h}/{j}.mkv"}
            for j in range(rng.choice([1, 1, 4, 8, 12]))
        ]
        e = {"id": 100000 + i, "auth_id": "00000000-0000-0000-0000-000000000000", "server": 11, "hash": h,
             "name": f"Release.Name.{i}.1080p.WEB-DL", "magnet": None, "size": sum(f["size"] for f in files),
             "active": False, "created_at": "2026-09-01T12:00:00+00:00", "updated_at": "2026-09-02T12:00:00+00:00",
             "download_state": "cached", "seeds": 0, "peers": 0, "ratio": 0.0, "progress": 1.0, "download_speed": 0,
             "upload_speed": 0, "eta": 0, "torrent_file": True, "expires_at": None, "download_present": True,
             "download_finished": True, "cached": True, "files": files, "tracker": None}
        entries.append(e)
        total += len(json.dumps(e, separators=(",", ":"))) + 1
    return {"success": True, "error": None, "detail": "Fetched torrents.", "data": entries}


def config():
    return {
        "debrid": [{"service": "torbox", "token": "t" * 40}, {"service": "realdebrid", "token": "R" * 52}],
        "indexers": ["torrentio", "comet", "mediafusion", "zilean"],
        "filters": {"excludeCam": True, "resolutions": ["2160p", "1080p"], "preferResolution": "1080p", "hdrOnly": False,
                    "minSeeders": 3, "maxSizeGB": 60, "excludeRegex": "(?i)\\b(korsub|hc)\\b"},
        # An install id is 16 bytes, base64url without padding.
        "iid": base64.urlsafe_b64encode(bytes(range(16))).decode().rstrip("="), "ep": 3,
    }


def write(name, value):
    path = OUT / name
    path.write_text(json.dumps(value, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    print(f"{path} {path.stat().st_size} bytes")


rel = releases(2000)
write("releases.json", rel)
write("streams-500.json", stremio(rel[:500]))
write("streams-50.json", stremio(rel[:50]))
write("torbox-listing.json", torbox_listing(10 * (1 << 20)))
write("config.json", config())
