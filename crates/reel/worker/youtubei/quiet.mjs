// Imported first by main.mjs: stdout is the protocol, one JSON reply per line, and nothing else may
// write to it. Library logging (console.log / info / debug) goes to stderr instead, where Reel
// inherits it into its own log; a stray line on stdout would be read as a reply and take the
// worker down.
const toStderr = (...args) => console.error(...args);
console.log = toStderr;
console.info = toStderr;
console.debug = toStderr;
