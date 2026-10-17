#!/bin/sh
# Exercise the source-built software ffmpeg that Reel and Subtitles share. The third argument is a
# full ffmpeg used only to generate deterministic inputs; no output depends on its codecs at runtime.
set -eu

ffmpeg=${1:-ffmpeg}
ffprobe=${2:-ffprobe}
generator=${3:-/usr/bin/ffmpeg}
root=$(mktemp -d)
trap 'test -n "${server_pid:-}" && kill "$server_pid" 2>/dev/null || true; rm -rf "$root"' EXIT

"$ffmpeg" -hide_banner -filters 2>/dev/null | grep -q cropdetect
"$ffmpeg" -hide_banner -muxers 2>/dev/null | grep -q ' s16le '
"$ffmpeg" -hide_banner -protocols 2>/dev/null | grep -q '^  http$'

# The repository corpus covers the containers and codecs Den actually sees. Probe every valid file,
# decode every audio-bearing file to Subtitles' mono 8 kHz WAV, and crop-detect every H.264 video.
find crates/remux/testdata crates/scout/testdata -type f \( -name '*.mkv' -o -name '*.mp4' -o -name '*.avi' \) \
  ! -name tracks.mkv | sort > "$root/media"
while IFS= read -r media; do
  "$ffprobe" -v error "$media" >/dev/null
  if "$ffprobe" -v error -select_streams a:0 -show_entries stream=codec_name -of csv=p=0 "$media" | grep -q .; then
    "$ffmpeg" -v error -nostdin -i "$media" -map 0:a:0 -t 0.5 -vn -ac 1 -ar 8000 \
      -c:a pcm_s16le -y "$root/audio.wav"
  fi
  if test "$("$ffprobe" -v error -select_streams v:0 -show_entries stream=codec_name -of csv=p=0 "$media")" = h264; then
    "$ffmpeg" -v error -nostdin -skip_frame nokey -i "$media" -an \
      -vf cropdetect=limit=24:round=2:reset=1 -f null - >/dev/null
  fi
done < "$root/media"

# Audio releases are broader than the checked-in corpus. Generate every codec the service contract
# names, then prove each can be decoded. Experimental flags affect fixture encoding only.
for spec in aac:aac ac3:ac3 eac3:eac3 dts:dca truehd:truehd mlp:mlp flac:flac opus:libopus \
  mp3:libmp3lame mp2:mp2 vorbis:libvorbis pcm16:pcm_s16le pcm24:pcm_s24le pcm32:pcm_s32le; do
  name=${spec%%:*}; codec=${spec#*:}; strict=
  case "$codec" in dca|truehd|mlp) strict='-strict -2' ;; esac
  # shellcheck disable=SC2086 -- strict deliberately expands to zero or two arguments.
  "$generator" -v error -y -f lavfi -i sine=frequency=440:sample_rate=48000:duration=1 -ac 2 \
    $strict -c:a "$codec" "$root/audio-$name.mkv"
  "$ffmpeg" -v error -nostdin -i "$root/audio-$name.mkv" -map 0:a:0 -ac 1 -ar 8000 \
    -c:a pcm_s16le -y "$root/decoded-$name.wav"
done

crop_boxes() {
  "$ffmpeg" -hide_banner -nostdin -skip_frame nokey -i "$1" -an \
    -vf cropdetect=limit=24:round=2:reset=1 -f null - 2>&1 >/dev/null \
    | sed -n 's/.*crop=\([0-9:]*\).*/\1/p' | sort | uniq -c | sed 's/^ *//'
}

assert_crop() {
  got=$(crop_boxes "$1")
  if test "$got" != "$2"; then
    echo "crop mismatch for $1" >&2
    echo "got:" >&2
    echo "$got" >&2
    echo "expected:" >&2
    echo "$2" >&2
    exit 1
  fi
}

"$generator" -v error -y -f lavfi -i testsrc=size=1920x816:rate=24:d=2 \
  -vf pad=1920:1080:0:132:color=black -c:v libx264 -threads 1 -g 6 -keyint_min 6 -sc_threshold 0 \
  -pix_fmt yuv420p "$root/letterbox.mp4"
"$generator" -v error -y -f lavfi -i testsrc=size=1920x816:rate=24:d=4 \
  -vf "pad=1920:1080:0:132:color=black,drawbox=x=40:y=20:w=420:h=90:color=white:t=fill:enable='between(t,3,4)'" \
  -c:v libx264 -threads 1 -g 6 -keyint_min 6 -sc_threshold 0 -pix_fmt yuv420p "$root/transient.mp4"
"$generator" -v error -y -f lavfi -i testsrc=size=1920x1080:rate=24:d=6 \
  -vf "drawbox=x=0:y=0:w=1920:h=131:color=black:t=fill:enable='lt(t,4.8)',drawbox=x=0:y=949:w=1920:h=131:color=black:t=fill:enable='lt(t,4.8)'" \
  -c:v libx264 -threads 1 -g 6 -keyint_min 6 -sc_threshold 0 -pix_fmt yuv420p "$root/mixed.mp4"
"$generator" -v error -y -f lavfi -i testsrc=size=720x404:rate=24:d=3 \
  -vf pad=720:1280:0:438:color=black -c:v libx264 -threads 1 -g 6 -keyint_min 6 -sc_threshold 0 \
  -pix_fmt yuv420p "$root/portrait.mp4"

assert_crop "$root/letterbox.mp4" '6 1920:816:0:132'
assert_crop "$root/transient.mp4" "2 1880:928:40:20
10 1920:816:0:132
2 1920:928:0:20"
assert_crop "$root/mixed.mp4" "1 1708:818:212:130
1 1738:818:182:130
1 1802:816:118:132
1 1864:816:56:132
4 1920:1080:0:0
14 1920:816:0:132"
assert_crop "$root/portrait.mp4" '10 640:404:80:438'

# Reel's cold merge is a video-only MP4 plus an audio-only M4A, copied to faststart MP4.
"$generator" -v error -y -f lavfi -i testsrc=size=320x180:rate=24:d=2 -c:v libx264 -threads 1 -an "$root/video.mp4"
"$generator" -v error -y -f lavfi -i sine=frequency=440:sample_rate=48000:duration=2 -c:a aac -vn "$root/audio.m4a"
"$ffmpeg" -v error -y -i "$root/video.mp4" -i "$root/audio.m4a" -map 0:v:0 -map 1:a:0 \
  -c copy -movflags +faststart "$root/trailer.mp4"
test "$("$ffprobe" -v error -show_entries stream=codec_name -of csv=p=0 "$root/trailer.mp4" | sort)" = "aac
h264"

# Subtitles reads through a guarded loopback HTTP relay, emits WAV, concatenates bounded samples,
# then alass asks ffmpeg for raw s16le. Exercise all three boundaries.
(cd "$root" && python3 -m http.server 18763 --bind 127.0.0.1 >/dev/null 2>&1) &
server_pid=$!
for _ in 1 2 3 4 5 6 7 8 9 10; do
  python3 -c 'import socket; s=socket.socket(); s.settimeout(.1); s.connect(("127.0.0.1",18763)); s.close()' \
    2>/dev/null && break
  sleep .1
done
"$ffmpeg" -v error -nostdin -i "http://127.0.0.1:18763/audio-eac3.mkv" -t 1 -map 0:a:0 -vn \
  -ac 1 -ar 8000 -c:a pcm_s16le -y "$root/sub.wav"
"$ffmpeg" -v error -nostdin -i "$root/sub.wav" -i "$root/sub.wav" \
  -filter_complex '[0:a][1:a]concat=n=2:v=0:a=1[out]' -map '[out]' -y "$root/concat.wav"
test "$("$ffmpeg" -v error -nostdin -i "$root/concat.wav" -map 0:a:0 -ac 1 -ar 8000 \
  -c:a pcm_s16le -f s16le - | wc -c)" -gt 30000

echo 'general ffmpeg: media=ok audio-codecs=14 crop=4 reel-merge=ok subtitle-http-concat-raw=ok'
