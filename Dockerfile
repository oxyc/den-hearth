# den-hearth — Scout, Reel, Subtitles and Remux in one binary, one image.
#
# One Rust binary (crates/hearth) and every tool the services shell out to (Scout shells out to none):
#   Reel       yt-dlp (+ its deno JS runtime, + a resident python resolver), youtubei.js (a resident deno
#              resolver, asked first), MP4Box (clap box), ffmpeg
#   Subtitles  alass, ffmpeg
#   Remux      a minimal, source-built ffmpeg with VAAPI, and Intel's media driver for a GPU transcode
#
# Two ffmpegs, on purpose. Remux's is built from source with everything switched off but what a session
# uses (its segment alignment depends on exactly how that build seeks, and its tests only count against
# it); Reel and Subtitles decode arbitrary media (cropdetect, any audio codec yt-dlp or a subtitle sync
# meets), which that build cannot. Debian's package serves those two. Remux's lives at
# /usr/local/bin/ffmpeg-remux and is the only one that is ever given the GPU.
#
# Runtime is debian-slim, not Alpine: the old Remux image was Alpine/musl, but Reel's MP4Box and deno
# are glibc builds, while its yt-dlp zipapp needs Python. Builds amd64, the box's arch.

# ---- build: Rust, with the dependency layer cached by cargo-chef ----------------------------------
FROM rust:1-trixie AS chef
RUN cargo install cargo-chef --version 0.1.78 --locked
WORKDIR /src

# The recipe is the workspace's manifests and lockfile only, so the layer below is keyed on dependencies:
# a code-only change recompiles the workspace crates, not the dependency tree, and a release (which moves
# only VERSION) recompiles nothing.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json -p den-hearth
COPY . .
# Empty in every release build. bench/http builds with `stub-upstreams`, which points the upstream bases at local stubs.
ARG HEARTH_FEATURES=""
RUN cargo build --release --locked -p den-hearth ${HEARTH_FEATURES:+--features "$HEARTH_FEATURES"}   # `strip = true` in the release profile

# ---- build MP4Box (GPAC) — writes the clap box; gpac is gone from Debian repos ----------------------
# Plain default build → MP4Box + libgpac.so (~10 MB total), linking only libc/libm/libz. Copying the
# two artifacts keeps the runtime debian-slim instead of pulling gpac's ~200-package apt tree.
FROM debian:trixie-slim AS mp4box
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential zlib1g-dev git ca-certificates && rm -rf /var/lib/apt/lists/*
ARG GPAC_VERSION=v2.4.0
# The commit that tag named when it was pinned. A tag can be moved and a commit cannot, and this source
# is compiled into the image with nothing else checking it — the same reason yt-dlp and deno are
# checksummed below.
ARG GPAC_COMMIT=5d70253ac94e5840be7b86054131dd753af63cc7
RUN git clone --depth 1 --branch ${GPAC_VERSION} https://github.com/gpac/gpac.git /gpac \
    && test "$(git -C /gpac rev-parse HEAD)" = "${GPAC_COMMIT}" \
    && cd /gpac && ./configure && make -j"$(nproc)"

# ---- fetch Reel's extractor tools (curl/unzip stay OUT of the runtime image) ------------------------
FROM debian:trixie-slim AS tools
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# JS runtime for yt-dlp. Recent yt-dlp REQUIRES one to solve YouTube's signature/nsig challenge —
# without it extraction is deprecated, formats go missing, and playback fails intermittently. deno
# is the one yt-dlp enables by default.
ARG DENO_VERSION=2.9.7
# Checksummed like yt-dlp below: this binary executes YouTube's JS in our container, so it is the
# last thing that should arrive unverified.
ARG DENO_SHA256_AMD64=c6527f24f4b16031d3ae4fa9f658d5f11534c8d84ce7dc8502420280919c3490
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) arch=x86_64-unknown-linux-gnu; sha=$DENO_SHA256_AMD64 ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://github.com/denoland/deno/releases/download/v${DENO_VERSION}/deno-${arch}.zip" \
      -o /tmp/deno.zip; \
    echo "${sha}  /tmp/deno.zip" | sha256sum -c -; \
    unzip -q -d /usr/local/bin /tmp/deno.zip

# Pinned yt-dlp zipapp. System Python executes this file for the CLI fallback, and the resident
# resolver imports the very same file. Keeping one verified artifact avoids carrying yt-dlp's
# PyInstaller build and its second bundled Python alongside the system Python the resolver needs.
# Bump YTDLP_VERSION to update (YouTube changes often — that's the whole maintenance burden, and
# it's yt-dlp's, not ours).
ARG YTDLP_VERSION=2026.08.19
# SHA256 from the release's SHA2-256SUMS. Verifying the download is the supply-chain guard — there
# is no package-manager advisory database involved. Kept in lockstep with YTDLP_VERSION by
# .github/workflows/ytdlp-update.yml.
ARG YTDLP_SHA256_ZIPAPP=1fa6733c37ea6fb51c99ad8fe785e7b7e5f3246c9b980230329d4fb72ed8d4d6
RUN set -eux; \
    curl -fsSL "https://github.com/yt-dlp/yt-dlp/releases/download/${YTDLP_VERSION}/yt-dlp" \
      -o /usr/local/bin/yt-dlp; \
    echo "${YTDLP_SHA256_ZIPAPP}  /usr/local/bin/yt-dlp" | sha256sum -c -; \
    chmod +x /usr/local/bin/yt-dlp

# youtubei.js, Reel's fast resolver (crates/reel/worker/youtubei). Its four npm packages are fetched HERE,
# at build time, with the deno above — the one runtime the image already trusts — into a cache the runtime
# stage copies and the running container only reads: nothing is installed at runtime (`--cached-only`), and
# a package that is not in the lockfile, or whose bytes do not match its integrity hash, is not loaded
# (`--frozen`). The lockfile is the pin; its SHA-256 below ties it to this Dockerfile the way the sums above
# tie the binaries, so a changed lockfile cannot ride in without this line changing with it. Kept in
# lockstep with deno.json + deno.lock by .github/workflows/youtubei-update.yml.
ARG YOUTUBEI_LOCK_SHA256=9bcd07416709f28439d51d1164045bcd3f7cd6c97b91f6125151a31e816fc7b5
COPY crates/reel/worker/youtubei/ /youtubei/
RUN set -eux; \
    cd /youtubei; \
    echo "${YOUTUBEI_LOCK_SHA256}  deno.lock" | sha256sum -c -; \
    DENO_DIR=/youtubei/deno deno cache --quiet --frozen --config=deno.json --lock=deno.lock main.mjs

# ---- build alass (static Rust CLI) — Subtitles' aligner -----------------------------------------------
FROM rust:1-trixie AS alass
# Pinned: unpinned, every rebuild shipped whatever the registry served that day, and nothing tells you
# when an aligner's behaviour changed. Bump deliberately.
ARG ALASS_VERSION=2.0.0
RUN cargo install alass-cli --version ${ALASS_VERSION} --root /alass --locked

# ---- build Remux's ffmpeg ---------------------------------------------------------------------------
# Debian's ffmpeg package brings a few hundred MB of codecs, filters and X libraries Remux never
# touches. This one is built from source with everything disabled, then only what a session uses
# switched back on: the Matroska and MP4 demuxers, the hls/mp4 muxers, the audio decoders a release can
# carry, the native AAC encoder, HTTP(S), and the VAAPI pieces of a GPU transcode. VAAPI is why the
# runtime carries libva and Intel's driver: libva loads the GPU's driver at run time.
FROM debian:trixie-slim AS ffmpeg
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential nasm pkgconf libssl-dev zlib1g-dev libva-dev xz-utils wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# Checksummed: this source is compiled into the image and parses untrusted media, with nothing else
# checking it. Dependabot has no ecosystem for it; bump both lines together.
ARG FFMPEG_VERSION=9.0.1
ARG FFMPEG_SHA256=cf38e0e28c7e5605942c4a77755349b0145804a397af37eb1fb4c77cb237f635
RUN wget -q -O /tmp/ffmpeg.tar.xz "https://ffmpeg.org/releases/ffmpeg-${FFMPEG_VERSION}.tar.xz" \
    && echo "${FFMPEG_SHA256}  /tmp/ffmpeg.tar.xz" | sha256sum -c - \
    && tar -xJf /tmp/ffmpeg.tar.xz -C /tmp
WORKDIR /tmp/ffmpeg-${FFMPEG_VERSION}
# What each group is for:
#   demuxers   matroska, mov                  — the containers scout's releases come in
#   muxers     hls (+ mp4, which it drives)   — fMP4 HLS output; pcm_s16le (`-f s16le` at run time), raw PCM on a
#              pipe for the intro/credits fingerprint (`segments`)
#   decoders   every audio codec a release carries: AC-3/E-AC-3, DTS (core of DTS-HD), TrueHD, FLAC,
#              Opus, AAC, MP3/MP2, Vorbis, LPCM. And h264/hevc: a copy needs them for stream probing
#              (to learn the B-frame reorder delay — without them ffmpeg guesses every copied packet's
#              DTS), and a transcode decodes through them with the VAAPI hwaccels below.
#              and the text subtitles a release carries — SRT, ASS/SSA, WebVTT, mov_text — which are written as WebVTT
#              beside the video in the same run (bitmap subtitles have no decoder here: nothing maps them)
#   encoders   aac (ffmpeg's native, stereo 192k); h264_vaapi for a transcode; webvtt for the subtitles; pcm_s16le
#              for the fingerprint's PCM
#   parsers    for the codecs the demuxers hand over unparsed. av1: Matroska asks for AV1's headers parsed, and
#              the parser reads the pixel format and colours from the sequence header. A copy of AV1 needs no
#              decoder: its packets are never reordered, so there is no delay to learn by probing
#   filters    the audio graph -ac 2 builds (resample/downmix, format negotiation, the buffer
#              endpoints every graph has); scale_vaapi and tonemap_vaapi for a transcode
#   bsfs       dovi_rpu and filter_units: a copied Dolby Vision stream loses its RPU and enhancement layer
#              and its configuration record, leaving the HDR10 base layer a browser can play
#   protocols  file, pipe (the fingerprint's PCM on stdout), and http(s) over tcp/tls (OpenSSL); --enable-version3 is
#              what OpenSSL 3 requires
# ffmpeg's own libraries are linked in statically; OpenSSL, zlib and libva are the system's.
RUN ./configure \
      --prefix=/opt/ffmpeg \
      --enable-static --disable-shared --enable-small \
      --disable-everything --disable-autodetect --disable-doc --disable-debug \
      --disable-ffplay --disable-avdevice --disable-swscale \
      --enable-version3 --enable-openssl --enable-zlib --enable-vaapi \
      --enable-protocol=file,pipe,http,https,tcp,tls \
      --enable-demuxer=matroska,mov \
      --enable-muxer=hls,mp4,webvtt,pcm_s16le \
      --enable-decoder=h264,hevc,aac,ac3,eac3,dca,truehd,mlp,flac,opus,mp3,mp2,vorbis,pcm_s16le,pcm_s24le,pcm_s32le,pcm_f32le,pcm_s16be,pcm_s24be,subrip,ass,ssa,webvtt,movtext \
      --enable-hwaccel=h264_vaapi,hevc_vaapi \
      --enable-encoder=aac,h264_vaapi,webvtt,pcm_s16le \
      --enable-parser=av1,h264,hevc,aac,ac3,dca,mlp,flac,opus,mpegaudio,vorbis \
      --enable-filter=aresample,aformat,anull,atrim,abuffer,abuffersink,null,trim,buffer,buffersink,format,scale_vaapi,tonemap_vaapi \
      --enable-bsf=dovi_rpu,filter_units \
      --enable-swresample \
    && make -j"$(nproc)" && make install

# ---- test: Remux's #[ignore]d end-to-end tests against its own ffmpeg ---------------------------------
# `docker build --target e2e .` (CI runs it; the default build skips it). The segment alignment depends on
# exactly how ffmpeg seeks, so these tests only count with the ffmpeg the image ships. ffprobe is built for
# them alone; the runtime image does not carry it. No GPU here: the transcode's flags are unit-tested.
FROM chef AS e2e
RUN apt-get update && apt-get install -y --no-install-recommends libssl3t64 zlib1g libva2 libva-drm2 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=ffmpeg /opt/ffmpeg/bin/ffmpeg /opt/ffmpeg/bin/ffprobe /usr/local/bin/
COPY . .
RUN cargo test --locked -p den-remux -- --include-ignored

# ---- release metadata ---------------------------------------------------------------------------------
# The runtime copies this marker, so a build without a valid DEN_VERSION fails before an image can be pushed.
# The version only reaches the final image's configuration and never invalidates the Rust or tool builds.
FROM debian:trixie-slim AS release-metadata
ARG DEN_VERSION
RUN printf '%s\n' "$DEN_VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' \
    || { echo "DEN_VERSION must be X.Y.Z, got '$DEN_VERSION'" >&2; exit 1; }; \
    printf '%s' "$DEN_VERSION" > /den-version

# ---- runtime ------------------------------------------------------------------------------------------
FROM debian:trixie-slim AS runtime

# ffmpeg (Reel and Subtitles) + ca-certificates (TLS roots) + python3 (Reel's resident resolver:
# spawning yt-dlp pays interpreter and import startup for every trailer). The libraries
# Remux's ffmpeg links are named here rather than left to ffmpeg's own dependencies. On amd64 — the box,
# with its UHD 630 — Intel's VAAPI driver (iHD) comes too, the non-free build (non-free component enabled for
# that install only): the free `intel-media-va-driver` exposes only the low-power encode entrypoint, which
# offers CQP alone ("Driver does not support any RC mode compatible with selected options"), and has no
# VPP kernels, so scale_vaapi fails too. Remux's bitrate-controlled encode needs the full one. `upgrade` first: the slim base is refreshed
# only every few weeks, and ffmpeg parses untrusted media, so a build takes the Debian security fixes
# current when this layer was last built. CI reuses the layer while the base digest is unchanged; the
# weekly patch rebuild runs without cache and so always takes the current ones.
RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
         ffmpeg ca-certificates python3 libssl3t64 zlib1g libva2 libva-drm2 \
    && if [ "$(dpkg --print-architecture)" = amd64 ]; then \
         sed -i 's/^Components: main$/Components: main non-free/' /etc/apt/sources.list.d/debian.sources \
         && apt-get update \
         && apt-get install -y --no-install-recommends intel-media-va-driver-non-free \
         && sed -i 's/^Components: main non-free$/Components: main/' /etc/apt/sources.list.d/debian.sources; \
       fi \
    && rm -rf /var/lib/apt/lists/*

COPY --from=tools /usr/local/bin/deno /usr/local/bin/deno
COPY --from=tools /usr/local/bin/yt-dlp /usr/local/bin/yt-dlp
# The process that imports the same zipapp. Executing this file is the fallback when the resident
# resolver cannot be started or has died.
COPY crates/reel/worker/resolve.py /app/resolve.py
# The PR image build is the yt-dlp update gate. Prove both uses of the single artifact here, without
# relying on YouTube being reachable from a CI address.
RUN cli="$(yt-dlp --version)" \
    && imported="$(python3 -c 'import sys; sys.path.insert(0, "/usr/local/bin/yt-dlp"); import yt_dlp; print(yt_dlp.version.__version__)')" \
    && test "$cli" = "$imported"
# The fast resolver, the packages it was locked to, and deno's cache of them (`deno/`, which the worker is
# pointed at as DENO_DIR and which only it uses: yt-dlp's own deno keeps its cache elsewhere).
COPY --from=tools /youtubei/main.mjs /youtubei/quiet.mjs /youtubei/errors.mjs /youtubei/select.mjs /youtubei/deno.json /youtubei/deno.lock /app/youtubei/
COPY --from=tools /youtubei/deno /app/youtubei/deno
# MP4Box + its shared lib (clap writer). ldconfig regenerates the libgpac.so.12 SONAME link.
COPY --from=mp4box /gpac/bin/gcc/MP4Box /usr/local/bin/MP4Box
COPY --from=mp4box /gpac/bin/gcc/libgpac.so.12.* /usr/local/lib/
RUN ldconfig
COPY --from=alass /alass/bin/alass-cli /usr/local/bin/alass
COPY --from=ffmpeg /opt/ffmpeg/bin/ffmpeg /usr/local/bin/ffmpeg-remux
COPY --from=build /src/target/release/den-hearth /usr/local/bin/den-hearth

# The release, last: everything above is keyed on content, so this only changes the image configuration.
# All three services report DEN_VERSION (startup line, manifests, /metrics).
ARG DEN_VERSION
ARG GIT_REVISION
ENV DEN_VERSION=$DEN_VERSION
LABEL org.opencontainers.image.version=$DEN_VERSION \
      org.opencontainers.image.revision=$GIT_REVISION
COPY --from=release-metadata /den-version /den-version

# Non-root, with the uid every den addon image uses (distroless's `nonroot`, 65532), so the box chowns
# one uid for every writable host dir. A real account rather than a bare number: deno keeps its cache
# under $HOME. /cache is created owned by it with one directory per service, so a fresh volume mounted
# at /cache is writable (the services create their own directory on first use).
RUN useradd --system --uid 65532 --user-group --create-home --home-dir /home/nonroot nonroot \
    && mkdir -p /cache/scout /cache/reel /cache/subs /cache/remux && chown -R nonroot:nonroot /cache

WORKDIR /app
# hearth's own PORT default (8090) applies, and each service also listens on the port its old container used
# (HEARTH_<SERVICE>_PORT, see README, Listeners). Everything below is per-service (see README, Configuration):
# the services share names such as CACHE_DIR and FFMPEG_PATH, so each gets its own prefixed copy.
# SCOUT_CACHE_DIR is Scout's own directory, never the Go den-scout's: the two disk formats are not proven
# compatible, so the Go container (kept stopped as the rollback) keeps its own cache.
# LIBVA_DRIVER_NAME: the UHD 630's driver, named, so libva does not probe for others; only Remux's
# ffmpeg ever opens a VAAPI device.
ENV SCOUT_CACHE_DIR=/cache/scout \
    REEL_CACHE_DIR=/cache/reel \
    REEL_FFMPEG_PATH=/usr/bin/ffmpeg \
    REEL_YTDLP_PATH=/usr/local/bin/yt-dlp \
    REEL_YTDLP_WORKER=/app/resolve.py \
    REEL_YOUTUBEI_WORKER=/app/youtubei/main.mjs \
    REEL_DENO_PATH=/usr/local/bin/deno \
    REEL_MAX_HEIGHT=1080 \
    SUBS_CACHE_DIR=/cache/subs \
    SUBS_FFMPEG_PATH=/usr/bin/ffmpeg \
    SUBS_ALASS_PATH=/usr/local/bin/alass \
    REMUX_SCRATCH_DIR=/cache/remux \
    REMUX_FFMPEG_PATH=/usr/local/bin/ffmpeg-remux \
    LIBVA_DRIVER_NAME=iHD
VOLUME ["/cache"]
# 8090 is the combined listener (/scout, /reel, /subs, /remux); the others are the old containers' ports:
# Scout 8080, Reel 8092, Subtitles 8093, Remux 8095.
EXPOSE 8090 8080 8092 8093 8095

# /health answers 200 whenever the process is up (degraded or not), and runs each service's own cheap
# health check. The old per-service images carried no HEALTHCHECK because a periodic probe keeps an idle box
# awake; this one is slow on purpose (5 min) and is the container runtime's view of "is it serving". Deploys
# still check /health themselves (den-update). No curl in the image, so python3 makes the request.
HEALTHCHECK --interval=5m --timeout=5s --start-period=30s --retries=2 \
  CMD ["python3", "-c", "import os,urllib.request as u; u.urlopen('http://127.0.0.1:'+os.environ.get('PORT','8090')+'/health', timeout=4)"]

USER 65532:65532
ENTRYPOINT ["den-hearth"]
