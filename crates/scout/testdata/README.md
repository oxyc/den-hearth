# Probe fixtures

Real container files, deliberately tiny (a 1-second 64×64 test pattern). They exist so the parsers are
tested against what a **muxer actually writes**, not against bytes hand-assembled to match the parser's
own assumptions — the failure mode where a fixture and the code drift together and the test keeps
passing.

The synthetic boxes in `probe_mp4_test.go` cover the opposite half: truncated sizes, 64-bit box headers,
a v1 `mdhd`, a `moov` that isn't there. A real muxer will never produce those, so they can't be fixtures.

| File | What it pins |
|---|---|
| `tracks.mkv` | Matroska track parsing (the EBML tree walk) |
| `xvid.avi` | RIFF/AVI, and the MPEG-4 Part 2 codec tag that routes playback to software decode |
| `sample.mp4` | A plain single-track MP4 |
| `multi-audio.mp4` | Two audio tracks with distinct languages (swe, ita) and channel counts (5.1, 2.0) |
| `moov-at-end.mp4` | A muxer that leaves `moov` at the end — the case a bounded head read cannot see |
| `hevc10.mp4` | HEVC Main 10 tagged `hvc1`: the bit depth read from `hvcC` |
| `hi10p.mkv` | H.264 High 10 in Matroska: the bit depth read from CodecPrivate (an `avcC` record) |

There is no Dolby Vision fixture. ffmpeg only writes a DV configuration when it is given an RPU, which a
test source can't provide. So the DOVIDecoderConfigurationRecord parser, and the MP4 and Matroska paths
to it, are tested against byte slices in `attributes_web_test.go`.

## Regenerating

Any ffmpeg will do; this is the container used, so the output is reproducible without installing one:

```sh
cd internal/scout/testdata

# multi-audio.mp4 — faststart, so moov lands at the FRONT (byte ~36)
docker run --rm -v "$PWD":/out -w /out linuxserver/ffmpeg:latest \
  -f lavfi -i testsrc=size=64x64:rate=5:duration=1 \
  -f lavfi -i sine=frequency=440:duration=1 -f lavfi -i sine=frequency=880:duration=1 \
  -map 0:v -map 1:a -map 2:a -c:v libx264 -preset ultrafast -c:a aac -ac:a:0 6 -ac:a:1 2 \
  -metadata:s:a:0 language=swe -metadata:s:a:1 language=ita \
  -movflags +faststart -y multi-audio.mp4

# moov-at-end.mp4 — NO faststart, so moov lands at the end (byte ~16128 of ~17.7 KB)
docker run --rm -v "$PWD":/out -w /out linuxserver/ffmpeg:latest \
  -f lavfi -i testsrc=size=64x64:rate=5:duration=1 -f lavfi -i sine=frequency=440:duration=1 \
  -map 0:v -map 1:a -c:v libx264 -preset ultrafast -c:a aac -metadata:s:a:0 language=fra \
  -y moov-at-end.mp4
```

The two 10-bit files need an ffmpeg with libx264 and libx265 (nixpkgs' has both). The audio track is
there because a Matroska head with no audio or subtitle track is reported as having no tracks:

```sh
cd internal/scout/testdata

# hevc10.mp4 — ~14 KB
nix shell nixpkgs#ffmpeg -c ffmpeg -f lavfi -i testsrc=size=64x64:rate=5:duration=1 \
  -f lavfi -i sine=frequency=440:duration=1 -map 0:v -map 1:a \
  -c:v libx265 -pix_fmt yuv420p10le -x265-params log-level=error -tag:v hvc1 \
  -c:a aac -metadata:s:a:0 language=eng -movflags +faststart -y hevc10.mp4

# hi10p.mkv — ~19 KB
nix shell nixpkgs#ffmpeg -c ffmpeg -f lavfi -i testsrc=size=64x64:rate=5:duration=1 \
  -f lavfi -i sine=frequency=440:duration=1 -map 0:v -map 1:a \
  -c:v libx264 -preset ultrafast -pix_fmt yuv420p10le -profile:v high10 \
  -c:a flac -metadata:s:a:0 language=jpn -y hi10p.mkv
```

`multi-audio.mp4` was encoded from `testsrc`'s RGB without a `-pix_fmt`, so libx264 chose 4:4:4 and
High 4:4:4 Predictive. That profile allows either bit depth, so its depth stays unknown, and the test
pins that too.

`moov-at-end.mp4` is only useful while its `moov` stays past the truncation point the test reads
(4 KiB). If a future ffmpeg reorders it, the test that asserts "a head-only read reports nothing" will
start failing for the right reason — regenerate, and check where `moov` landed:

```sh
python3 -c "d=open('moov-at-end.mp4','rb').read(); print('moov at', d.find(b'moov'), 'of', len(d))"
```
