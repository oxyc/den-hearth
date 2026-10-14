// Candidate D: youtubei.js on deno as a resident process, speaking the same line protocol as
// crates/reel/worker/resolve.py (one JSON request per line in, one reply per line out).
//
//   {"id": "...", "client": "TV", "po_token": "...", "visitor_data": "..."}  ->  {"ok": true, "height", "width", "urls": [video, audio], "hls"}
//
// Run from a directory whose node_modules holds youtubei.js (and bgutils-js + jsdom for BgUtils):
//   deno run -A --node-modules-dir=manual ytjs_worker.mjs
import { Innertube, UniversalCache } from "youtubei.js";
import readline from "node:readline";

const MAX_HEIGHT = 1080;
let yt;

async function innertube() {
  yt ??= await Innertube.create({ cache: new UniversalCache(false), retrieve_player: true, generate_session_locally: false });
  return yt;
}

function pick(formats, wantVideo) {
  const mine = formats.filter((f) =>
    wantVideo
      ? f.mime_type?.includes("avc1") && (f.height ?? 0) <= MAX_HEIGHT
      : f.mime_type?.includes("mp4a"),
  );
  mine.sort((a, b) => (wantVideo ? (b.height ?? 0) - (a.height ?? 0) : 0) || (b.bitrate ?? 0) - (a.bitrate ?? 0));
  return mine[0];
}

async function answer(req) {
  const tube = await innertube();
  const info = await tube.getBasicInfo(req.id, { client: req.client || "TV" });
  const status = info.playability_status?.status;
  if (status && status !== "OK") {
    return { ok: false, error: `ERROR: [youtube] ${req.id}: ${status} ${info.playability_status?.reason ?? ""}` };
  }
  const adaptive = info.streaming_data?.adaptive_formats ?? [];
  const video = pick(adaptive, true);
  const audio = pick(adaptive, false);
  if (!video || !audio) return { ok: false, error: `ERROR: no avc1+mp4a formats (${adaptive.length} adaptive)` };
  const urls = [];
  for (const f of [video, audio]) {
    const url = await f.decipher(tube.session.player);
    if (!url) return { ok: false, error: "ERROR: could not decipher a stream url" };
    urls.push(url);
  }
  return { ok: true, width: video.width, height: video.height, urls, hls: info.streaming_data?.hls_manifest_url ?? null };
}

const lines = readline.createInterface({ input: process.stdin });
for await (const line of lines) {
  if (!line.trim()) continue;
  let reply;
  try {
    reply = await answer(JSON.parse(line));
  } catch (e) {
    reply = { ok: false, error: `${e?.constructor?.name}: ${e?.message}` };
  }
  process.stdout.write(JSON.stringify(reply) + "\n");
}
