// deno test errors.test.mjs
// Each line must classify (ytdlp::classify) the way yt-dlp's own text for the same failure does.
import assert from "node:assert/strict";
import { thrown, unplayable } from "./errors.mjs";

const id = "dQw4w9WgXcQ";

Deno.test("a bot check reads as a throttle", () => {
  const line = unplayable(id, "LOGIN_REQUIRED", "Sign in to confirm you’re not a bot");
  assert.match(line.toLowerCase(), /confirm you’re not a bot/);
});

Deno.test("an age gate reads as restricted", () => {
  assert.match(unplayable(id, "LOGIN_REQUIRED", "This video may be inappropriate for some users. Sign in to confirm your age").toLowerCase(), /sign in to confirm your age/);
});

Deno.test("a private video reads as private", () => {
  assert.match(unplayable(id, "UNPLAYABLE", "This video is private.").toLowerCase(), /private video/);
});

Deno.test("a removed video reads as removed", () => {
  assert.match(unplayable(id, "ERROR", "This video has been removed by the uploader").toLowerCase(), /removed by the uploader/);
  assert.match(unplayable(id, "ERROR", "This video is no longer available").toLowerCase(), /has been removed/);
});

Deno.test("a geo block keeps YouTube's wording", () => {
  const line = unplayable(id, "UNPLAYABLE", "The uploader has not made this video available in your country");
  assert.match(line.toLowerCase(), /available in your country/);
});

Deno.test("an empty reason is still unavailable", () => {
  assert.match(unplayable(id, "ERROR", "").toLowerCase(), /video unavailable/);
});

Deno.test("an HTTP 429 reads as a throttle", () => {
  assert.match(thrown(id, new Error("Request failed with status 429")).toLowerCase(), /http error 429/);
});

Deno.test("a thrown 'video is unavailable' reads as unavailable", () => {
  assert.match(thrown(id, new Error("This video is unavailable")).toLowerCase(), /video unavailable/);
});

Deno.test("anything else is a plain extraction failure", () => {
  const line = thrown(id, new TypeError("x is undefined"));
  assert.match(line, /TypeError: x is undefined/);
  assert.doesNotMatch(line.toLowerCase(), /429|unavailable|private|removed|country|bot/);
});
