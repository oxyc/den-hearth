// Which streams answer a request: `config::format_ladder`, rung by rung, over youtubei.js's formats.
// Pure, so `select.test.mjs` can hold it to the shapes YouTube really returns.

const has = (f, codec) => (f.mime_type ?? "").includes(codec);
const best = (list, by) => list.reduce((a, b) => (a && by(a) >= by(b) ? a : b), null);

export function chooseVideo(adaptive, limit) {
  const videos = adaptive.filter((f) => f.has_video && !f.has_audio && has(f, "avc1") && (f.height ?? 0) <= limit);
  // Tallest, then smoothest, then richest: yt-dlp's own order for `bv*`.
  return best(videos, (f) => (f.height ?? 0) * 1e9 + (f.fps ?? 0) * 1e6 + (f.bitrate ?? 0));
}

/** The video's own sound: not an auto-dub, not a translation, not the default-less duplicate tracks. */
const isOriginal = (f) =>
  f.audio_track?.audio_is_default !== false && f.is_original !== false && !f.is_dubbed && !f.is_auto_dubbed;

/**
 * The audio to pair with a video: the original language track, not the dynamic-range-compressed
 * copy of it, then the richest of those. A video with auto-dubs carries a stream per language at
 * the same itag, and a dub can out-bitrate the original (a German dub at 131963 against the
 * English original at 131818), so bitrate alone picks the wrong voice. yt-dlp sorts language
 * before quality for the same reason. When nothing is left after a filter the next, looser one
 * applies, so a video whose every track is flagged still gets sound.
 */
export function chooseAudio(adaptive) {
  const audios = adaptive.filter((f) => f.has_audio && !f.has_video && has(f, "mp4a"));
  const pool = [
    audios.filter((f) => isOriginal(f) && !f.is_drc),
    audios.filter(isOriginal),
    audios.filter((f) => !f.is_drc),
    audios,
  ].find((tier) => tier.length);
  return best(pool ?? [], (f) => f.bitrate ?? 0);
}

export function chooseMuxed(muxed, limit) {
  const both = muxed.filter((f) => f.has_video && f.has_audio && has(f, "avc1") && has(f, "mp4a"));
  const within = both.filter((f) => (f.height ?? 0) <= limit);
  return best(within, (f) => (f.height ?? 0) * 1e9 + (f.bitrate ?? 0));
}

/** The formats to hand back for these rungs (tallest first), or none. */
export function select(streaming, rungs) {
  const adaptive = streaming.adaptive_formats ?? [];
  const muxed = streaming.formats ?? [];
  for (const limit of rungs) {
    const video = chooseVideo(adaptive, limit);
    const audio = chooseAudio(adaptive);
    if (video && audio) return [video, audio];
    const whole = chooseMuxed(muxed, limit);
    if (whole) return [whole];
  }
  const itag18 = muxed.find((f) => f.itag === 18);
  if (itag18) return [itag18];
  const any = muxed.find((f) => f.has_video && f.has_audio && has(f, "avc1") && has(f, "mp4a"));
  return any ? [any] : [];
}
