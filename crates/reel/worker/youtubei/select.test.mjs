// deno test select.test.mjs
// Fixtures are shaped like what youtubei.js returns for a VISIONOS player call (field names and the
// per-language mp4a streams at one itag), with the bitrates that made the first version pick wrong.
import assert from "node:assert/strict";
import { chooseAudio, select } from "./select.mjs";

const audio = (itag, bitrate, extra = {}) => ({
  itag, bitrate, has_audio: true, has_video: false, mime_type: 'audio/mp4; codecs="mp4a.40.2"',
  is_drc: false, is_dubbed: false, is_auto_dubbed: false, is_original: true, ...extra,
});
const track = (id, audio_is_default) => ({ audio_track: { id, audio_is_default } });
const video = (itag, height, extra = {}) => ({
  itag, height, width: Math.round(height * 16 / 9), fps: 24, bitrate: height * 4000, has_video: true, has_audio: false,
  mime_type: 'video/mp4; codecs="avc1.640028"', ...extra,
});

// A trailer with auto-dubs: the original (en-US.4, the default) and seven dubs per itag, one of them
// out-bitrating the original, plus the compressed-range copy of the original.
const dubbed = [
  audio(140, 131963, { ...track("de-DE.10", false), is_original: false, is_auto_dubbed: true }),
  audio(140, 130585, { ...track("es-US.10", false), is_original: false, is_auto_dubbed: true }),
  audio(140, 131818, track("en-US.4", true)),
  audio(139, 50013, track("en-US.4", true)),
  audio(139, 49896, { ...track("de-DE.10", false), is_original: false, is_auto_dubbed: true }),
];

Deno.test("an auto-dub with a higher bitrate does not beat the original track", () => {
  const chosen = chooseAudio(dubbed);
  assert.equal(chosen.audio_track.id, "en-US.4");
  assert.equal(chosen.itag, 140);
});

Deno.test("the compressed-range copy of a track does not beat the plain one", () => {
  const formats = [audio(140, 130784, { is_drc: true, is_original: false }), audio(140, 130772)];
  assert.equal(chooseAudio(formats).bitrate, 130772);
});

Deno.test("a single untagged track is the track", () => {
  assert.equal(chooseAudio([audio(139, 50000), audio(140, 130000)]).itag, 140);
});

Deno.test("when every track is a dub, the best dub still gives sound", () => {
  const onlyDubs = dubbed.filter((f) => f.is_auto_dubbed);
  assert.equal(chooseAudio(onlyDubs).bitrate, 131963);
});

Deno.test("a track marked non-default is passed over even if nothing else flags it", () => {
  const formats = [audio(140, 131963, track("fr-FR.1", false)), audio(140, 131818, track("en-US.4", true))];
  assert.equal(chooseAudio(formats).audio_track.id, "en-US.4");
});

Deno.test("select pairs the tallest allowed avc1 with the original audio", () => {
  const streaming = {
    adaptive_formats: [...dubbed, video(137, 1080), video(136, 720), video(135, 480), video(248, 1080, { mime_type: 'video/webm; codecs="vp9"' })],
    formats: [],
  };
  const [v, a] = select(streaming, [1080, 720, 480]);
  assert.equal(v.itag, 137);
  assert.equal(a.audio_track.id, "en-US.4");
  assert.equal(select(streaming, [720, 480])[0].itag, 136);
  assert.equal(select(streaming, [480])[0].itag, 135);
});

Deno.test("with no video at the rung it falls to itag 18, then to nothing", () => {
  const muxed = { itag: 18, height: 360, has_video: true, has_audio: true, mime_type: 'video/mp4; codecs="avc1.42001E, mp4a.40.2"' };
  assert.deepEqual(select({ adaptive_formats: [], formats: [muxed] }, [1080, 720]).map((f) => f.itag), [18]);
  assert.deepEqual(select({ adaptive_formats: [], formats: [] }, [1080]), []);
});
