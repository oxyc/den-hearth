// What youtubei.js says went wrong, in the words yt-dlp would have used.
//
// Reel classifies every extractor failure from the text of the message (`ytdlp::classify`), so the
// resident yt-dlp's own error lines and this worker's must land in the same categories. YouTube's
// `playabilityStatus.reason` is already what yt-dlp prints for most of them; the few it paraphrases
// are normalised below. `errors.test.mjs` pins each mapping.

/** The error line for a video YouTube would not play, as `ERROR: [youtube] <id>: <reason>`. */
export function unplayable(id, status, reason) {
  const text = String(reason ?? "").trim();
  const lowered = text.toLowerCase();
  let said = text;
  if (/\bprivate\b/.test(lowered)) said = "Private video. Sign in if you've been granted access to this video";
  else if (/age/.test(lowered) && /sign in|confirm/.test(lowered)) said = "Sign in to confirm your age";
  else if (/\bremoved\b|no longer available/.test(lowered)) said = `${text} (has been removed)`;
  else if (!text) said = status === "LOGIN_REQUIRED" ? "Sign in required" : "Video unavailable";
  return `ERROR: [youtube] ${id}: ${said}`;
}

/** The error line for a request that threw. HTTP 429 reads as yt-dlp's own throttle wording. */
export function thrown(id, error) {
  const message = String(error?.message ?? error);
  if (/\b429\b|too many requests/i.test(message)) {
    return `ERROR: [youtube] ${id}: HTTP Error 429: Too Many Requests`;
  }
  // youtubei.js throws, rather than reporting a status, for a video the API has no record of.
  if (/this video is unavailable/i.test(message)) return `ERROR: [youtube] ${id}: Video unavailable`;
  return `ERROR: [youtube] ${id}: ${error?.constructor?.name ?? "Error"}: ${message}`;
}
