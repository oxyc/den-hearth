//! The streamed decode of TorBox's account listing (Go `decodeListing`, `skipValue`, `truncationDetector`,
//! `listingFault`, `listingFaultFor`, `bodyEndedEarly`).
//!
//! Go walks the body with `json.Decoder`'s `Token`/`More`/`Decode`, which is what keeps a 64 MiB listing from
//! being held in memory: only the hash to id map survives. Rust has no token-level decoder, and serde's visitors
//! cannot tell Go's four ways of failing apart, so this is a small reader over `std::io::Read` that follows the
//! same walk and returns the same verdicts. It keeps nothing but the map, a 32 KiB buffer and, for one entry, a
//! hash that fits an infohash.
//!
//! Where Go distinguishes an error by its type, the reader distinguishes it by `Stop`: `Early` is what Go
//! reports as `io.EOF`, `io.ErrUnexpectedEOF` or a failed read (the input stopped, so a retry may succeed), and
//! `Syntax` is what it reports as a `*json.SyntaxError` or `*json.UnmarshalTypeError` (the body is the wrong
//! shape and will be again).

use std::collections::HashMap;
use std::io::Read;

use crate::play::{MAX_INFO_HASH_LEN, MIN_INFO_HASH_LEN};

use super::MAX_LISTING_ENTRIES;

/// Go `maxSkipDepth` bounds how deeply an unknown field may nest before `skip_value` refuses it.
///
/// That walk allocates nothing itself, but Decoder.Token pushes onto its own token stack for every open
/// bracket, and that stack is LIVE rather than garbage, so GOMEMLIMIT cannot reclaim it. Measured on
/// `{"x":[[[[…` STREAMED off a socket, which is how production reads it: 1 MiB of brackets peaks at
/// 25 MiB, 4 MiB at 95 MiB, and 10 MiB at ~231 MiB — past the 230 MiB limit, on a body a SIXTH of the
/// byte cap. (The Rust walk keeps a byte per open bracket, so the bound is about the verdict now, not the
/// heap; it is kept because the verdict is part of the contract.)
///
/// A real listing does not nest: skip_value is reached only for an unknown TOP-LEVEL key, since `data` is
/// opened by token and decoded element by element, so nothing in a real entry is measured against this at
/// all. Sixty-four is far past anything TorBox could reasonably send.
///
/// Refusing reads as `ListingFault::BadEnvelope` — remembered for the TTL, not retried. Two reasons, and the
/// earlier note here got both backwards:
///
/// It is deterministic. The same body arrives next poll and refuses at the same bracket, so retrying only
/// repeats the cost. That cost is NOT the "walk stops at the 65th bracket" this comment used to claim:
/// keys are consumed in stream order, so the walk stops early only when the deep field PRECEDES `data`.
/// With it after `data` — the ordering any real body has, since `data` is what the listing is for — the
/// whole body has already been read by the time skip_value sees a bracket at all. Measured on a
/// 2,000-entry fixture: the full 0.58 MiB read, every poll, silently.
///
/// And the same shape one level in is memoised already. Nesting inside `data[]` is decoded rather than
/// skipped, so it meets the decoder's own 10,000-level ceiling and comes back a syntax error, which
/// `listing_fault_for` maps to `BadEnvelope`. Leaving the top-level bound on the transient road made two
/// spellings of one hostile body behave oppositely.
///
/// The risk memoising carries — a bound too tight for a legitimate body locks the account out for the
/// whole TTL — is pinned from below by `skip_value_accepts_depth_just_under_the_bound` rather than argued
/// about: this cannot be lowered without a red test.
pub(crate) const MAX_SKIP_DEPTH: usize = 64;

/// Go `encoding/json`'s `maxNestingDepth`: the deepest value its scanner accepts.
const MAX_NESTING_DEPTH: usize = 10_000;

/// Go `listingFault` says WHY a listing could not be read, for the reasons that are properties of the
/// listing rather than blips. Each must be remembered: re-pulling a body that will fail the same way is
/// the egress the oversized memo exists to stop. Only a transient failure — a timeout, a 5xx — is worth
/// retrying at once, because that retry is the only thing that rediscovers a queued torrent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ListingFault {
    /// No PERSISTENT fault. Most returns carrying this are still failures — a decode error, a truncated
    /// body — they are simply the retryable kind. Named for the absence of a fault rather than for success,
    /// because reading it as "the listing parsed" is a trap: the parse result is the separate ok value.
    None,
    /// The body was well-formed and the wrong SHAPE. Two families reach it: an envelope with no usable
    /// `data` array — the key renamed, `data` holding an object, an explicit `data:null`, an explicit
    /// success:false — and a field of the wrong type, `id` as a string or `success` as one. Same
    /// deterministic class as the two below, and separated in both families from a truncated body, which
    /// reaches the same places and is not.
    ///
    /// success:false is the arguable member, and it is arguable in one direction only. TorBox signals state
    /// in band at HTTP 200 — a torrent still downloading answers exactly that — so if mylist ever returns
    /// it for something transient, a rate limit or a maintenance window, that blip is remembered for the
    /// listing TTL. Kept here because the cost is bounded and asymmetric: ok=false stays indeterminate, no
    /// miss marker is written, so nothing ever concludes the account does not hold a release, and what is
    /// lost is fifteen seconds of rediscovery. Treating it as transient instead reinstates the 45 fetches
    /// and 539 MiB per wait this fault was added to remove.
    BadEnvelope,
    /// More entries than the map may hold.
    TooManyEntries,
    /// Entries arrived and not one was usable. An upstream that renamed `hash` looks exactly like this,
    /// and it will look like it again on the next poll, so it is remembered rather than re-fetched.
    NoUsableEntries,
}

/// Why a read of the JSON stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    /// The input ended, or could not be read: a blip.
    Early,
    /// The input is the wrong thing: malformed JSON, or JSON of the wrong type for the field.
    Syntax,
}

/// How `skip_value` stopped, which is `Stop` plus the one verdict the depth bound adds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Skip {
    Early,
    Syntax,
    TooDeep,
}

impl From<Stop> for Skip {
    fn from(s: Stop) -> Skip {
        match s {
            Stop::Early => Skip::Early,
            Stop::Syntax => Skip::Syntax,
        }
    }
}

/// Go `truncationDetector` reports whether a LimitReader was consumed all the way to its limit, which is the
/// only way to tell "the body ended" from "the cap cut it off" — and those two must not look alike.
///
/// It is given a reader that stops at `limit + 1` bytes, so reading exactly the cap and being cut off at it
/// are different counts. Go counts the bytes itself (`n > limit`). The count is the reader's own here: a
/// `Take` knows how many bytes it has left, and it has none left exactly when `limit + 1` went through. That is
/// the same observation, and it leaves the struct a plain `{ r, limit }`.
pub(crate) struct TruncationDetector<R> {
    pub(crate) r: R,
    pub(crate) limit: usize,
}

/// How many bytes a limiting reader will still hand out.
pub(crate) trait Remaining {
    fn remaining(&self) -> u64;
}

impl<T> Remaining for std::io::Take<T> {
    fn remaining(&self) -> u64 {
        self.limit()
    }
}

impl<R: Read> Read for TruncationDetector<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.r.read(buf)
    }
}

impl<R: Remaining> TruncationDetector<R> {
    /// Go `truncationDetector.truncated`: `limit + 1` bytes went through.
    pub(crate) fn truncated(&self) -> bool {
        self.r.remaining() == 0
    }
}

const BUF_LEN: usize = 32 * 1024;

/// The bytes that end a run of plain string text: the closing quote, a backslash, and the control characters
/// JSON forbids raw.
static ENDS_RUN: [bool; 256] = {
    let mut t = [false; 256];
    let mut i = 0;
    while i < 0x20 {
        t[i] = true;
        i += 1;
    }
    t[b'"' as usize] = true;
    t[b'\\' as usize] = true;
    t
};

/// A reader over the body that parses JSON a piece at a time.
struct Reader<R> {
    r: R,
    buf: Vec<u8>,
    pos: usize,
    end: usize,
    /// The input ended or failed; nothing more will be read.
    done: bool,
    /// `skip_value`'s stack of open containers, kept between calls.
    open: Vec<bool>,
}

impl<R: Read> Reader<R> {
    fn new(r: R) -> Reader<R> {
        Reader { r, buf: vec![0; BUF_LEN], pos: 0, end: 0, done: false, open: Vec::new() }
    }

    /// Reads the next block. False at the end of the input or on a read error, both of which Go's decoder
    /// reports as the input having stopped.
    fn fill(&mut self) -> bool {
        if self.done {
            return false;
        }
        loop {
            match self.r.read(&mut self.buf) {
                Ok(0) => {
                    self.done = true;
                    return false;
                }
                Ok(n) => {
                    self.pos = 0;
                    self.end = n;
                    return true;
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    self.done = true;
                    return false;
                }
            }
        }
    }

    /// The next byte without consuming it; None where the input has stopped.
    fn peek(&mut self) -> Option<u8> {
        if self.pos == self.end && !self.fill() {
            return None;
        }
        Some(self.buf[self.pos])
    }

    fn bump(&mut self) {
        self.pos += 1;
    }

    /// The next byte, consumed.
    fn next_byte(&mut self) -> Result<u8, Stop> {
        let c = self.peek().ok_or(Stop::Early)?;
        self.bump();
        Ok(c)
    }

    /// The next byte that is not whitespace, not consumed. Go's `dec.peek()`.
    #[inline]
    fn skip_ws(&mut self) -> Option<u8> {
        loop {
            if self.pos == self.end && !self.fill() {
                return None;
            }
            match self.buf[self.pos] {
                b' ' | b'\t' | b'\n' | b'\r' => self.pos += 1,
                c => return Some(c),
            }
        }
    }

    /// Consumes the ASCII digits that come next, a block at a time, handing each to `digit`.
    #[inline]
    fn digits(&mut self, mut digit: impl FnMut(u8)) {
        loop {
            if self.pos == self.end && !self.fill() {
                return;
            }
            let chunk = &self.buf[self.pos..self.end];
            let n = chunk.iter().position(|b| !b.is_ascii_digit()).unwrap_or(chunk.len());
            chunk[..n].iter().for_each(|&d| digit(d));
            self.pos += n;
            if self.pos < self.end {
                return;
            }
        }
    }

    /// Reads the literal `rest` (the bytes after its first, which the caller has seen).
    fn literal(&mut self, word: &[u8]) -> Result<(), Stop> {
        for &want in word {
            if self.next_byte()? != want {
                return Err(Stop::Syntax);
            }
        }
        Ok(())
    }

    /// Reads a string whose opening quote is next, handing its decoded bytes to `sink` a run at a time, and
    /// checks it to its closing quote. What the sink keeps is its business: [`Discard`] keeps nothing and
    /// [`Stack`] keeps a bounded prefix, so no string is ever copied to the heap here.
    ///
    /// Bytes are taken a block at a time, so a 60 MiB string costs one pass and no allocation.
    fn string_with<S: Sink>(&mut self, sink: &mut S) -> Result<(), Stop> {
        self.bump(); // the opening quote
        loop {
            if self.pos == self.end && !self.fill() {
                return Err(Stop::Early);
            }
            let chunk = &self.buf[self.pos..self.end];
            let len = chunk.len();
            let run = chunk.iter().position(|&b| ENDS_RUN[usize::from(b)]).unwrap_or(len);
            sink.push(&chunk[..run]);
            self.pos += run;
            if run == len {
                continue;
            }
            match self.buf[self.pos] {
                b'"' => {
                    self.bump();
                    return Ok(());
                }
                b'\\' => {
                    self.bump();
                    let mut escaped = Esc { b: [0; 8], n: 0 };
                    self.escape(&mut escaped)?;
                    sink.push(&escaped.b[..escaped.n]);
                }
                // A raw control character inside a string is not JSON.
                _ => return Err(Stop::Syntax),
            }
        }
    }

    /// A string whose text nobody wants.
    fn skip_string(&mut self) -> Result<(), Stop> {
        self.string_with(&mut Discard)
    }

    /// Reads the escape after a backslash into `out`, as UTF-8.
    fn escape(&mut self, out: &mut Esc) -> Result<(), Stop> {
        let mut c = self.next_byte()?;
        loop {
            let ch = match c {
                b'"' => '"',
                b'\\' => '\\',
                b'/' => '/',
                b'b' => '\u{8}',
                b'f' => '\u{c}',
                b'n' => '\n',
                b'r' => '\r',
                b't' => '\t',
                b'u' => {
                    let code = self.hex4()?;
                    if !(0xD800..0xDC00).contains(&code) {
                        push_code(out, code);
                        return Ok(());
                    }
                    // A high surrogate pairs with a low one that follows as its own escape.
                    if self.peek() != Some(b'\\') {
                        push_code(out, code);
                        return Ok(());
                    }
                    self.bump();
                    let second = self.next_byte()?;
                    if second != b'u' {
                        // Not a pair: the lone surrogate is a replacement character, and what follows is an
                        // escape of its own.
                        push_code(out, code);
                        c = second;
                        continue;
                    }
                    let low = self.hex4()?;
                    if (0xDC00..0xE000).contains(&low) {
                        let pair = 0x10000 + ((u32::from(code) - 0xD800) << 10) + (u32::from(low) - 0xDC00);
                        push_char(out, char::from_u32(pair).unwrap_or('\u{FFFD}'));
                    } else {
                        push_code(out, code);
                        push_code(out, low);
                    }
                    return Ok(());
                }
                _ => return Err(Stop::Syntax),
            };
            push_char(out, ch);
            return Ok(());
        }
    }

    fn hex4(&mut self) -> Result<u16, Stop> {
        let mut v: u16 = 0;
        for _ in 0..4 {
            let digit = match self.next_byte()? {
                c @ b'0'..=b'9' => c - b'0',
                c @ b'a'..=b'f' => c - b'a' + 10,
                c @ b'A'..=b'F' => c - b'A' + 10,
                _ => return Err(Stop::Syntax),
            };
            v = v << 4 | u16::from(digit);
        }
        Ok(v)
    }

    /// Reads a number whose first byte is next, checking Go's scanner grammar, and returns its value when it
    /// is an integer that fits an int64: what `str::parse::<i64>` of the number's text would give. A fraction,
    /// an exponent or an overflow is None, a value that is valid JSON and not an int64.
    ///
    /// A number cut off where it could not yet be complete (`-`, `1.`, `1e`) is `Early`; one that simply ends
    /// the input is complete, which is how Go scans a bare scalar at EOF.
    fn number(&mut self) -> Result<Option<i64>, Stop> {
        let negative = self.peek() == Some(b'-');
        if negative {
            self.bump();
        }
        // Accumulated towards the sign it will have, so i64::MIN fits.
        let mut value: Option<i64> = Some(0);
        let accumulate = |acc: &mut Option<i64>, d: u8| {
            let d = i64::from(d - b'0');
            *acc = acc.and_then(|a| a.checked_mul(10)).and_then(|a| {
                if negative {
                    a.checked_sub(d)
                } else {
                    a.checked_add(d)
                }
            });
        };
        match self.next_byte()? {
            c @ b'0' => accumulate(&mut value, c),
            c @ b'1'..=b'9' => {
                accumulate(&mut value, c);
                self.digits(|d| accumulate(&mut value, d));
            }
            _ => return Err(Stop::Syntax),
        }
        let mut integer = true;
        if self.peek() == Some(b'.') {
            integer = false;
            self.bump();
            match self.next_byte()? {
                b'0'..=b'9' => {}
                _ => return Err(Stop::Syntax),
            }
            self.digits(|_| {});
        }
        if let Some(b'e' | b'E') = self.peek() {
            integer = false;
            self.bump();
            if let Some(b'+' | b'-') = self.peek() {
                self.bump();
            }
            match self.next_byte()? {
                b'0'..=b'9' => {}
                _ => return Err(Stop::Syntax),
            }
            self.digits(|_| {});
        }
        Ok(if integer { value } else { None })
    }

    /// Go `skipValue` walks one JSON value without materialising it, so an unknown field costs no retention.
    ///
    /// `max_depth` separates the failures for the caller: past it is a deterministic property of the body,
    /// while running out of input is the truncated read that must be retried. Structure is checked as it
    /// goes (commas, colons, string keys), as Go's `Token` does.
    fn skip_value(&mut self, max_depth: usize) -> Result<(), Skip> {
        // The kind of each open container is `self.open`: true for an object. It is the reader's own, so an
        // entry's nested fields reuse one allocation instead of making a stack each.
        self.open.clear();
        loop {
            // A value is next.
            match self.skip_ws().ok_or(Skip::Early)? {
                b'{' => {
                    self.bump();
                    if self.open.len() + 1 > max_depth {
                        return Err(Skip::TooDeep);
                    }
                    if self.skip_ws().ok_or(Skip::Early)? == b'}' {
                        self.bump();
                    } else {
                        self.open.push(true);
                        self.skip_key()?;
                        continue;
                    }
                }
                b'[' => {
                    self.bump();
                    if self.open.len() + 1 > max_depth {
                        return Err(Skip::TooDeep);
                    }
                    if self.skip_ws().ok_or(Skip::Early)? == b']' {
                        self.bump();
                    } else {
                        self.open.push(false);
                        continue;
                    }
                }
                b'"' => {
                    self.skip_string()?;
                }
                b'-' | b'0'..=b'9' => {
                    self.number()?;
                }
                b't' => {
                    self.bump();
                    self.literal(b"rue")?;
                }
                b'f' => {
                    self.bump();
                    self.literal(b"alse")?;
                }
                b'n' => {
                    self.bump();
                    self.literal(b"ull")?;
                }
                _ => return Err(Skip::Syntax),
            }
            // A value is complete: close what it closes, or move to the next sibling.
            loop {
                let Some(&is_object) = self.open.last() else {
                    return Ok(());
                };
                match (is_object, self.skip_ws().ok_or(Skip::Early)?) {
                    (true, b'}') | (false, b']') => {
                        self.bump();
                        self.open.pop();
                    }
                    (true, b',') => {
                        self.bump();
                        self.skip_key()?;
                        break;
                    }
                    (false, b',') => {
                        self.bump();
                        break;
                    }
                    _ => return Err(Skip::Syntax),
                }
            }
        }
    }

    /// An object key and its colon, read and discarded.
    fn skip_key(&mut self) -> Result<(), Skip> {
        if self.skip_ws().ok_or(Skip::Early)? != b'"' {
            return Err(Skip::Syntax);
        }
        self.skip_string()?;
        self.colon()?;
        Ok(())
    }

    /// The colon after an object key.
    fn colon(&mut self) -> Result<(), Stop> {
        if self.skip_ws().ok_or(Stop::Early)? != b':' {
            return Err(Stop::Syntax);
        }
        self.bump();
        Ok(())
    }

    /// Go `dec.Decode(&v)` into a `*bool`: true, false or null. Any other value is read to its end — a syntax
    /// error in it wins, as Go validates the whole value before it checks types — and is then a type error.
    fn decode_optional_bool(&mut self) -> Result<Option<bool>, Stop> {
        match self.skip_ws().ok_or(Stop::Early)? {
            b't' => {
                self.bump();
                self.literal(b"rue")?;
                Ok(Some(true))
            }
            b'f' => {
                self.bump();
                self.literal(b"alse")?;
                Ok(Some(false))
            }
            b'n' => {
                self.bump();
                self.literal(b"ull")?;
                Ok(None)
            }
            _ => {
                self.skip_value(MAX_NESTING_DEPTH).map_err(skip_to_stop)?;
                Err(Stop::Syntax)
            }
        }
    }

    /// Go `dec.Decode(&e)` for one `{id, hash}` entry: keys match case-insensitively, a null leaves a field
    /// alone, and every other field is read and dropped without being kept.
    ///
    /// Each call starts from nothing. Entries do not carry fields over from the entry before them: an entry
    /// missing `hash` that inherited the previous one's would map a hash the account holds to a DIFFERENT
    /// torrent's id, which the status read then follows.
    fn entry(&mut self) -> Result<Entry, Stop> {
        let mut e = Entry::default();
        match self.skip_ws().ok_or(Stop::Early)? {
            b'n' => {
                self.bump();
                self.literal(b"ull")?;
                return Ok(e);
            }
            b'{' => self.bump(),
            _ => {
                // Not an object: read it whole, then it is a type error.
                self.skip_value(MAX_NESTING_DEPTH).map_err(skip_to_stop)?;
                return Err(Stop::Syntax);
            }
        }
        let mut type_error = false;
        if self.skip_ws().ok_or(Stop::Early)? == b'}' {
            self.bump();
            return Ok(e);
        }
        loop {
            if self.skip_ws().ok_or(Stop::Early)? != b'"' {
                return Err(Stop::Syntax);
            }
            let mut key = Stack::<16>::new();
            self.string_with(&mut key)?;
            self.colon()?;
            let field = key.bytes().and_then(entry_field);
            let c = self.skip_ws().ok_or(Stop::Early)?;
            match field {
                Some(EntryField::Id) => match c {
                    b'n' => {
                        self.bump();
                        self.literal(b"ull")?;
                    }
                    b'-' | b'0'..=b'9' => match self.number()? {
                        Some(id) => e.id = id,
                        None => type_error = true,
                    },
                    _ => {
                        self.skip_value(MAX_NESTING_DEPTH - 1).map_err(skip_to_stop)?;
                        type_error = true;
                    }
                },
                Some(EntryField::Hash) => match c {
                    b'n' => {
                        self.bump();
                        self.literal(b"ull")?;
                    }
                    // The cap is what keeps a 60 MiB "hash" from being retained: 40 runes of at most 4 bytes
                    // each is the most that can lower into range, so a longer value is unusable whatever it
                    // lowers to, and it is refused here without being copied.
                    b'"' => {
                        let mut text = Stack::<HASH_CAP>::new();
                        self.string_with(&mut text)?;
                        e.hash = if text.over { Hash::TooLong } else { Hash::Text(text) };
                    }
                    _ => {
                        self.skip_value(MAX_NESTING_DEPTH - 1).map_err(skip_to_stop)?;
                        type_error = true;
                    }
                },
                None => self.skip_value(MAX_NESTING_DEPTH - 1).map_err(skip_to_stop)?,
            }
            match self.skip_ws().ok_or(Stop::Early)? {
                b',' => self.bump(),
                b'}' => {
                    self.bump();
                    break;
                }
                _ => return Err(Stop::Syntax),
            }
        }
        if type_error {
            return Err(Stop::Syntax);
        }
        Ok(e)
    }
}

/// `skip_value` where the caller has no use for the depth verdict: Go's scanner calls a body nested past its
/// ceiling a syntax error.
fn skip_to_stop(s: Skip) -> Stop {
    match s {
        Skip::Early => Stop::Early,
        Skip::Syntax | Skip::TooDeep => Stop::Syntax,
    }
}

/// The most bytes of a hash value worth keeping: 40 runes of at most 4 bytes each is the most that can lower
/// into range, so a longer value is unusable whatever it lowers to.
const HASH_CAP: usize = 4 * MAX_INFO_HASH_LEN;

/// Where a string's bytes go as it is read.
trait Sink {
    fn push(&mut self, bytes: &[u8]);
}

/// Keeps nothing.
struct Discard;

impl Sink for Discard {
    #[inline]
    fn push(&mut self, _: &[u8]) {}
}

/// Keeps the first `N` bytes on the stack. `over` says the string was longer, in which case nothing is kept.
struct Stack<const N: usize> {
    buf: [u8; N],
    len: usize,
    over: bool,
}

impl<const N: usize> Stack<N> {
    fn new() -> Self {
        Stack { buf: [0; N], len: 0, over: false }
    }

    /// The string; None when it did not fit.
    fn bytes(&self) -> Option<&[u8]> {
        if self.over {
            None
        } else {
            Some(&self.buf[..self.len])
        }
    }
}

impl<const N: usize> Sink for Stack<N> {
    #[inline]
    fn push(&mut self, bytes: &[u8]) {
        if self.over {
            return;
        }
        if self.len + bytes.len() > N {
            self.over = true;
            return;
        }
        self.buf[self.len..self.len + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
    }
}

/// One escape sequence's UTF-8: at most two replacement characters (a lone surrogate before another one).
struct Esc {
    b: [u8; 8],
    n: usize,
}

#[derive(Default)]
enum Hash {
    #[default]
    Unset,
    Text(Stack<HASH_CAP>),
    /// Longer than any hash could lower into range.
    TooLong,
}

#[derive(Default)]
struct Entry {
    id: i64,
    hash: Hash,
}

enum EntryField {
    Id,
    Hash,
}

/// Which of the entry's two fields a key names. Go's decoder matches a key to a field case-insensitively, by
/// simple folding, which adds U+017F (long s) to "s".
///
/// Works on the key's UTF-8 bytes: an ASCII letter folds to lower case, U+017F (C5 BF) is "s" and U+212A (E2 84
/// AA, the Kelvin sign) is "k", and anything else cannot be a letter of either name. The longest name is four
/// letters, so nothing is built to compare.
fn entry_field(key: &[u8]) -> Option<EntryField> {
    let mut folded = [0u8; 4];
    let mut n = 0;
    let mut i = 0;
    while i < key.len() {
        let (c, width) = match key[i..] {
            [0xC5, 0xBF, ..] => (b's', 2),
            [0xE2, 0x84, 0xAA, ..] => (b'k', 3),
            [b, ..] if b.is_ascii() => (b.to_ascii_lowercase(), 1),
            _ => return None,
        };
        if n == folded.len() {
            return None;
        }
        folded[n] = c;
        n += 1;
        i += width;
    }
    match &folded[..n] {
        b"id" => Some(EntryField::Id),
        b"hash" => Some(EntryField::Hash),
        _ => None,
    }
}

fn push_char(out: &mut Esc, c: char) {
    let mut b = [0u8; 4];
    let s = c.encode_utf8(&mut b).as_bytes();
    out.b[out.n..out.n + s.len()].copy_from_slice(s);
    out.n += s.len();
}

/// A UTF-16 code unit as UTF-8; a surrogate on its own is the replacement character.
fn push_code(out: &mut Esc, code: u16) {
    push_char(out, char::from_u32(u32::from(code)).unwrap_or('\u{FFFD}'));
}

/// The listing's hash as the map key: the decoded text lower-cased, None when it is not the length of an
/// infohash. An ASCII hash is checked by its length before anything is built and is allocated exactly
/// once, at its final size; a hash with other characters takes the slow road, where the lowered length is what
/// decides.
fn lowered_hash(raw: &[u8]) -> Option<String> {
    if raw.is_ascii() {
        if !(MIN_INFO_HASH_LEN..=MAX_INFO_HASH_LEN).contains(&raw.len()) {
            return None;
        }
        let mut s = String::with_capacity(raw.len());
        s.extend(raw.iter().map(|b| char::from(b.to_ascii_lowercase())));
        return Some(s);
    }
    let hash = String::from_utf8_lossy(raw).to_lowercase();
    (MIN_INFO_HASH_LEN..=MAX_INFO_HASH_LEN).contains(&hash.len()).then_some(hash)
}

/// Go `listingFaultFor` splits a decode error into the kind worth remembering and the kind worth retrying.
///
/// The line is drawn by measurement, not by guessing which errors look serious. Truncation INSIDE an
/// object or a string — cut mid-key, mid-number, mid-string, after a comma — returns io.ErrUnexpectedEOF,
/// and a COMPLETE body that is wrong returns one of the two below: a type error for valid JSON of the
/// wrong shape (`id` as a string), a syntax error for JSON that is not valid at all (a leading-zero
/// number, a trailing comma, an unquoted key, a bad escape). Both are deterministic — the same bytes
/// arrive next poll and fail the same way — and re-fetching them is the guaranteed waste, 45 fetches and
/// up to 538 MiB per wait, that every other deterministic fault here is memoised to avoid.
///
/// One exception, measured rather than assumed: a BARE scalar cut at EOF is scanned as a complete value,
/// so `{"data":[123` reports a type error rather than an unexpected EOF and is memoised. It makes no
/// difference here, because an entry that is a bare number is a schema change whether or not the body was
/// also cut, so both readings reach the same verdict.
///
/// A syntax error could in principle come from corruption in transit rather than from the upstream, which
/// a retry would clear. Remembering it anyway costs fifteen seconds of "could not find out" — no add, no
/// miss marker — against re-pulling a body that will not parse, on a two-second cadence.
///
/// This runs at the two DECODE sites only, so invalid JSON elsewhere — in an unknown field, an unquoted
/// top-level key, a trailing comma in the envelope — still retries. That asymmetry is deliberate: those
/// surface as token failures, and the shape that most often produces one is an HTML error page from an
/// intermediary, which is exactly the transient case a retry should clear. Widening this to token errors
/// would memoise a proxy hiccup for fifteen seconds to save re-reading a body the upstream will probably
/// stop sending anyway.
///
/// Go's `bodyEndedEarly` reports whether a decode error means the input stopped, rather than that it was wrong.
/// Go 1.26's decoder says so with io.EOF or io.ErrUnexpectedEOF. Go 1.27's reports a body that stops BETWEEN
/// values — after a complete entry, or right after `[` — as a *json.SyntaxError reading "unexpected end of JSON
/// input", which wraps neither; measured on both toolchains. Without this check listingFaultFor filed that blip
/// with the schema changes and memoised it for fifteen seconds, on exactly the retry that rediscovers a queued
/// torrent. The message is the one encoding/json has always used for truncated input (json.Unmarshal of a cut
/// body returns it on every version), and the cut cases in
/// TestAccountListing_remembersOversizedButRetriesTransient pin it. `Stop::Early` is that distinction made at
/// the source, so there is nothing to catch here.
fn listing_fault_for(stop: Stop) -> ListingFault {
    match stop {
        Stop::Early => ListingFault::None,
        Stop::Syntax => ListingFault::BadEnvelope,
    }
}

/// Go `decodeListing` walks `{"success":…,"data":[{id,hash},…]}` and keeps only the hash→id map.
///
/// Returns ok=false for anything it cannot read as a list. No list, no verdict: a 200 carrying no `data`
/// key at all, or an explicit success:false, is not the account saying it holds nothing — it is TorBox's
/// envelope missing, and listFiles reads exactly the same body as silence ("not a claim about the
/// account"). Calling that authoritative wrote a 15s miss marker that then suppressed the only lookup able
/// to rediscover a queued torrent.
///
/// The fault says whether ok=false is worth REMEMBERING; `ListingFault::None` there means "no persistent
/// fault", so an ordinary parse failure stays retryable.
pub(crate) fn decode_listing<R: Read>(r: R) -> (HashMap<String, i64>, bool, ListingFault) {
    match walk(&mut Reader::new(r)) {
        Ok(ids) => (ids, true, ListingFault::None),
        Err(fault) => (HashMap::new(), false, fault),
    }
}

fn walk<R: Read>(rd: &mut Reader<R>) -> Result<HashMap<String, i64>, ListingFault> {
    use ListingFault::{BadEnvelope, None as Retry};

    if rd.skip_ws() != Some(b'{') {
        return Err(Retry);
    }
    rd.bump();
    let mut success = true;
    let mut saw_data = false;
    let mut seen = 0usize;
    let mut ids: Option<HashMap<String, i64>> = None;
    let mut first = true;
    // Go's `for dec.More()`: more to read unless the next byte closes something, or there is no next byte.
    while let Some(c) = rd.skip_ws() {
        if c == b'}' || c == b']' {
            break;
        }
        if !first {
            if c != b',' {
                return Err(Retry);
            }
            rd.bump();
        }
        first = false;
        if rd.skip_ws() != Some(b'"') {
            return Err(Retry);
        }
        let mut key = Stack::<16>::new();
        rd.string_with(&mut key).map_err(|_| Retry)?;
        match key.bytes() {
            Some(b"success") => {
                // Same split as the entry decode below: `success` arriving as a string is a schema change,
                // a body cut off here is a blip.
                rd.colon().map_err(listing_fault_for)?;
                if let Some(v) = rd.decode_optional_bool().map_err(listing_fault_for)? {
                    success = v;
                }
            }
            Some(b"data") => {
                // A REPEATED `data` key makes the body unreadable, rather than picking one of them.
                //
                // Two rules were tried here and each was unsafe in one direction. "Last wins" lets a trailing
                // null erase a list already read, turning an answer into "no answer". "First non-null wins"
                // lets `{"data":[],"data":[…]}` settle on the empty one — an authoritative "this account
                // holds nothing", which writes a 15 s miss marker and costs a duplicate add. There is no
                // third rule that is safe for both, because the safe direction is opposite in each case.
                //
                // So neither: ok=false is indeterminate, which is the answer that concludes nothing and adds
                // nothing. TorBox does not emit duplicate keys, so this costs nothing in practice; what it
                // buys is that a body which does is never mistaken for a fact about the account.
                if saw_data {
                    return Err(Retry);
                }
                saw_data = true;
                // A null `data` is the envelope-missing case and must stay distinct from an empty array.
                if matches!(rd.skip_ws(), None | Some(b'}') | Some(b']')) {
                    return Err(Retry);
                }
                rd.colon().map_err(|_| Retry)?;
                match rd.skip_ws().ok_or(Retry)? {
                    b'n' => {
                        rd.bump();
                        rd.literal(b"ull").map_err(|_| Retry)?;
                        continue; // explicit null
                    }
                    b'[' => rd.bump(),
                    // `data` present and not an array — an object, a string, a number. Deterministic in the
                    // same way a renamed key is, and distinguishable from truncation, which errors on the
                    // token read above rather than yielding a well-formed one of the wrong kind.
                    b'{' => return Err(BadEnvelope),
                    b'"' => {
                        rd.skip_string().map_err(|_| Retry)?;
                        return Err(BadEnvelope);
                    }
                    b'-' | b'0'..=b'9' => {
                        rd.number().map_err(|_| Retry)?;
                        return Err(BadEnvelope);
                    }
                    b't' => {
                        rd.bump();
                        rd.literal(b"rue").map_err(|_| Retry)?;
                        return Err(BadEnvelope);
                    }
                    b'f' => {
                        rd.bump();
                        rd.literal(b"alse").map_err(|_| Retry)?;
                        return Err(BadEnvelope);
                    }
                    _ => return Err(Retry),
                }
                let mut map: HashMap<String, i64> = HashMap::new();
                let mut first_entry = true;
                while let Some(c) = rd.skip_ws() {
                    if c == b']' || c == b'}' {
                        break;
                    }
                    if !first_entry {
                        // Go's Decode wants the comma between elements itself.
                        if c != b',' {
                            return Err(BadEnvelope);
                        }
                        rd.bump();
                    }
                    first_entry = false;
                    // Decode fails for two unrelated reasons and they must not be collapsed. A body cut
                    // off mid-entry is a blip. An entry whose fields are the WRONG TYPE — `id` arriving
                    // as a string, `hash` as a number, an element that is a number or a string — is a
                    // schema change, exactly as deterministic as the renamed `hash` this function
                    // already memoises, and exactly the v2-API shape that comment invokes. (A `null`
                    // element is the one scalar that decodes cleanly: it carries no hash, so it is
                    // filtered like any other unusable entry rather than reaching here.)
                    //
                    // Collapsing them put a single odd entry back on the re-fetch road: measured at 45
                    // listing fetches and up to 538 MiB per wait, on a two-second cadence, forever —
                    // the same number this function was driven to 1 for every other deterministic
                    // shape.
                    let e = rd.entry().map_err(listing_fault_for)?;
                    // The cap counts entries SEEN, and is checked BEFORE the filter below can skip past it.
                    // The byte cap does not bound the entry count: 60 MiB of minimal entries is ~1M of them
                    // and peaked at 346 MiB against a 230 MiB GOMEMLIMIT. Counting only what survived the
                    // filter — or leaving this check below a `continue` — would let a body of a million
                    // unusable hashes fall through to an empty map and be reported as an authoritative
                    // "this account holds nothing", which costs a duplicate add: worse than the retention
                    // the filter was added to prevent.
                    seen += 1;
                    if seen > MAX_LISTING_ENTRIES {
                        return Err(ListingFault::TooManyEntries);
                    }
                    // A hash that could never be asked about is dropped rather than retained. Both caps miss
                    // this, because the key is whatever the upstream sent: one entry carrying a 60 MiB
                    // "hash" is one entry and 60 MiB, under each of them, and it decoded as a valid listing,
                    // retained the 60 MiB for the memo TTL, and allocated 248 MiB doing it.
                    //
                    // Nothing real is lost: every hash that can reach a lookup is 32 or 40 characters —
                    // play.go's infoHashRe on the /play path, scrape.go's hashNorm on the probe path, which
                    // does NOT go through infoHashRe. The bounds are shared with play.go so those two
                    // cannot drift; hashNorm is a third copy and is only kept honest by the test.
                    //
                    // Filtering NARROWER than the lookups accept is the direction that costs: a torrent the
                    // account holds becomes an authoritative miss and a fifteen-second marker.
                    //
                    // Two steps, because the length that decides is the LOWERED one: a non-ASCII hash can be
                    // 96 raw bytes and 32 after lowering, and judging it on the raw length would drop a key
                    // that is perfectly askable. The cheap guard first bounds what ToLower may copy — 40
                    // runes of at most 4 bytes each is the most that can lower into range — so the 60 MiB
                    // case is still refused without allocating. (Here that guard is the `string` cap in
                    // `entry`, which refuses the long value without keeping it.)
                    let Hash::Text(raw) = &e.hash else { continue };
                    let Some(hash) = lowered_hash(&raw.buf[..raw.len]) else { continue };
                    map.insert(hash, e.id);
                }
                // Entries arrived and NONE of them were usable: that is a listing we could not read, not an
                // account holding nothing. An empty map answers ok=true, which findTorrentByHash reports as
                // an authoritative miss and which writes a fifteen-second marker against a torrent the
                // account may well already hold.
                //
                // It does NOT save the add, and saying so here was wrong twice. Measured end to end: with
                // the listing unreadable, /play escalates once, the escalated read is indeterminate too,
                // and handler.go then resolves anyway — deliberately, because never starting the download
                // is worse than a duplicate. Both shapes spend the same one add. What this buys is the
                // marker: nothing records a false "the account does not hold this" for the next fifteen
                // seconds, and the answer stops claiming knowledge it does not have.
                //
                // The filter above made this reachable at any size, not just past the entry cap: if TorBox
                // renames `hash` — a v2 API, or `infohash` — every entry filters out at once. An account
                // that genuinely holds nothing sends no entries at all, so it still answers authoritatively.
                // The closing bracket FIRST: "no usable entries" is a statement about a complete array, and
                // the loop above ends for a read error exactly as it does for a closed one. Asking the
                // question before the array is known complete classified a body cut off after an unusable
                // entry as a persistent fault, memoising a blip for fifteen seconds and suppressing the one
                // retry that rediscovers a queued torrent — the same ordering mistake as the envelope check
                // below, one level in.
                if rd.skip_ws() != Some(b']') {
                    return Err(Retry); // closing ]
                }
                rd.bump();
                if seen > 0 && map.is_empty() {
                    return Err(ListingFault::NoUsableEntries);
                }
                ids = Some(map);
            }
            _ => {
                // Walked token by token, NOT decoded into a json.RawMessage. RawMessage keeps a full copy of
                // the field's raw bytes on top of the decoder's own buffer, so a large unknown top-level field
                // was retained twice: on a 63 MiB array field, 191 MiB allocated before against 67 MiB now.
                //
                // Both numbers are TotalAlloc, because mixing a peak against a residency is how this comment
                // was wrong before — it quoted 3.9 MiB "now", which is the post-GC figure and made the saving
                // look like 50x rather than the 2.9x it is. Nor is 67 MiB free: Token materialises every
                // string it steps over, so the walk allocates once per TOKEN and that ratio is a property of
                // the body's token density, not of the walk. 1.06x holds for the 6 KB strings measured here;
                // the same 24 MiB written as small tokens measures 13x to 52x depending on how tightly they
                // are packed, the top of that range being a bare `[0,0,0,…]` array. It is all garbage rather
                // than live heap, so it costs GC time and not GOMEMLIMIT headroom — which is the actual win
                // over holding the body twice at once, not a small constant.
                //
                // Nothing STRUCTURED is retained — a scalar is not helped, and saying otherwise would be the
                // kind of comment this file keeps having to correct. A single huge string costs the same
                // either way, because Token must buffer the whole token and allocate the string.
                //
                // That cost is 256.7 MiB: one value just under maxListingBytes, the decoder's buffer having
                // doubled to 128 MiB while the materialised string is live beside it, so the GC can reclaim
                // neither. That is above the 230 MiB GOMEMLIMIT and above the container, i.e. an OOM kill,
                // and it is not reduced by the length filter on `hash` above, which runs only after the token
                // has been materialised.
                //
                // This figure has now been wrong in BOTH directions, so how it is measured matters more than
                // the number. It was understated at ~159 MiB, corrected to 256.7, then "corrected" again to
                // 304 — and 304 was the mistake: that probe fed the decoder a strings.Reader, so the fixture
                // held the whole body live in the heap for the duration and the peak counted the body twice.
                // Production reads off a socket and never materialises the body. Measure it streamed, or the
                // number comes out high by exactly one body.
                //
                // Left as it is, deliberately: it needs a hostile or broken api.torbox.app rather than a caller
                // (the base URL is a constant), and the alternative is lowering maxListingBytes, which governs
                // real large accounts and would turn them oversized. Recorded in FOLLOWUP.md with the options
                // rather than traded for a functional regression on a threat this package already accepts.
                //
                // The NESTED shape is a different mechanism with a different answer — bounded by maxSkipDepth
                // and remembered rather than retried. See that constant.
                //
                // (All of the above is about Go's `json.Decoder`. This walk reads a block at a time and keeps a
                // byte per open bracket, so a string here costs its length only when it is a key or a hash,
                // both capped; the bound on depth is kept for the verdict.)
                rd.colon().map_err(|_| Retry)?;
                match rd.skip_value(MAX_SKIP_DEPTH) {
                    Ok(()) => {}
                    Err(Skip::TooDeep) => return Err(BadEnvelope),
                    Err(_) => return Err(Retry),
                }
            }
        }
    }
    // The loop ends either because the object closed or because the body ran out — it reports both as "no
    // more", so the closing token is what tells them apart. Reading it is the ONLY way to know the envelope
    // was complete, and that is what makes the difference between a persistent fault and a blip.
    //
    // It is also a CORRECTNESS fix, not only a cost one, which is worth saying before someone removes it as
    // an optimisation: without it `{"success":true,"data":[]` — an empty array with the body cut off before
    // the brace — parsed as an authoritative empty account and wrote a fifteen-second miss marker. A
    // truncated body claiming "the account does not hold this" is exactly the failure the surrounding code
    // spent several rounds removing, arrived at from the one direction nothing was watching.
    if rd.skip_ws() != Some(b'}') {
        return Err(Retry); // cut off mid-body: transient, retry at once
    }
    rd.bump();
    match ids {
        Some(ids) if success => Ok(ids),
        // A complete, well-formed envelope that simply has no usable `data` array — the key renamed, an
        // explicit `data:null`, or an explicit success:false. (A `data` that is present but not an array
        // is the same fault, returned earlier, where its own shape is known.) Deterministic: the same body
        // arrives next poll and fails the same way.
        //
        // This sat on the transient road while its sibling (entries present, none usable) was taken off
        // it, which is the same defect one level out: measured at 45 listing fetches over a 15-poll wait
        // and 539 MiB pulled on a 12 MiB account, for each of a renamed `data` key, a `data` object, and a
        // success:false. Not a blanket rule — a body cut off mid-stream reaches here too and IS transient,
        // which is what the token read above separates.
        _ => Err(BadEnvelope),
    }
}
