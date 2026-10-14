// A resident youtubei.js resolver, speaking the same line protocol as ../resolve.py:
//
//   {"id": "dQw4w9WgXcQ", "rungs": [1080, 720, 480]}
//   {"ok": true, "width": 1920, "height": 1080, "urls": [video, audio], "hls": "https://...", "itags": [137, 140]}
//   {"ok": false, "error": "ERROR: [youtube] ... Video unavailable"}
//
// It exists because one player call is all a resolve needs: yt-dlp spends ~1 s on a webpage, a player
// request and the HLS manifest, this spends ~0.12 s. Reel asks it first and falls back to yt-dlp on ANY
// failure, so it never has to be clever: a video it cannot answer is simply not its video.
//
// Selection mirrors `config::format_ladder`, rung by rung: the best avc1 video at or under the rung
// plus the best mp4a audio, else the best muxed avc1+mp4a at or under it; after the last rung, itag 18,
// else any muxed mp4 avc1+mp4a. `rungs` comes from Reel so the two cannot drift.
//
// Run from the packages deno.lock pins (see the Dockerfile) with network access to YouTube's API
// hosts only: no filesystem, no environment, no subprocesses.
import "./quiet.mjs"; // first: nothing but a reply may reach stdout
import { Innertube, UniversalCache } from "youtubei.js";
import readline from "node:readline";
import process from "node:process";
import { thrown, unplayable } from "./errors.mjs";
import { select } from "./select.mjs";

const CLIENT = "VISIONOS";
// A session carries a visitor identity; a long-lived one is what keeps answers whole (see the
// benchmark), but not forever. Rebuilt hourly, and at once after anything unexpected.
const SESSION_MAX_AGE_MS = 60 * 60 * 1000;

let tube = null;
let builtAt = 0;

async function session() {
  if (!tube || Date.now() - builtAt > SESSION_MAX_AGE_MS) {
    // No player: VISIONOS URLs arrive ready to fetch, so there is no signature to solve and no
    // base.js to download. A format that does need one fails below and Reel falls back to yt-dlp.
    tube = await Innertube.create({
      cache: new UniversalCache(false),
      enable_session_cache: false, // the default keeps one on disk under $TMPDIR
      retrieve_player: false,
    });
    builtAt = Date.now();
  }
  return tube;
}

async function answer(request) {
  const { id, rungs } = request;
  if (!/^[A-Za-z0-9_-]{11}$/.test(id ?? "")) return { ok: false, error: `ERROR: [youtube] ${id}: not a video id` };
  const yt = await session();
  const info = await yt.getBasicInfo(id, { client: CLIENT });
  const status = info.playability_status;
  if (status?.status && status.status !== "OK") {
    return { ok: false, error: unplayable(id, status.status, status.reason) };
  }
  const streaming = info.streaming_data;
  if (!streaming) return { ok: false, error: `ERROR: [youtube] ${id}: no streaming data` };
  const chosen = select(streaming, rungs?.length ? rungs : [1080, 720, 480]);
  if (!chosen.length) return { ok: false, error: `ERROR: [youtube] ${id}: Requested format is not available` };
  const urls = [];
  for (const format of chosen) {
    const url = format.url ?? (await format.decipher(undefined));
    if (!url) return { ok: false, error: `ERROR: [youtube] ${id}: a stream has no usable url` };
    urls.push(url);
  }
  const head = chosen[0];
  return {
    ok: true,
    width: head.width ?? null,
    height: head.height ?? null,
    urls,
    hls: streaming.hls_manifest_url ?? null,
    itags: chosen.map((f) => f.itag),
  };
}

const lines = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of lines) {
  if (!line.trim()) continue;
  let reply;
  let id = "?";
  try {
    const request = JSON.parse(line);
    id = request.id;
    reply = await answer(request);
  } catch (error) {
    tube = null; // unknown state: build a new session for the next one
    reply = { ok: false, error: thrown(id, error) };
  }
  process.stdout.write(JSON.stringify(reply) + "\n");
}
