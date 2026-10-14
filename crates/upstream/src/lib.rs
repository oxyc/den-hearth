//! Outbound HTTP and upstream backoff, written once.
//!
//! Reel, Subtitles, Remux and Scout each carried their own reqwest builders, their own pause for an upstream that
//! said to stop, and their own reading of `Retry-After`. Each was fixed on its own for its own reasons (a total
//! timeout that cut a paused player's relay, a redirect that carried a credential off its host, a burst of in-flight
//! refusals that doubled a pause once per request), and the fixes did not travel. This crate is where they live:
//!
//!  * [`ClientProfile`] names a client by its settings (connect, total or read-idle timeout, redirect policy, user
//!    agent, address guard). A service defines its profiles once and builds one client per profile.
//!  * [`AddressGuard`], [`PublicOnly`] and the profile's [`Guard`] keep a request off the LAN, the loopback and the
//!    tailnet, judged by the one classifier in `den-net`.
//!  * [`retry_after`], [`exhausted_for`] and [`throttle`] read what a response says about waiting.
//!  * [`Pauses`] remembers it, per credential.
//!  * [`civil`] is the calendar arithmetic under every timestamp the services parse.

pub mod civil;
pub mod client;
pub mod guard;
pub mod headers;
pub mod pause;

pub use client::{ClientProfile, Guard, Redirect};
pub use guard::{AddressGuard, GuardError, PublicOnly, Vetted, NOT_PUBLIC};
pub use headers::{exhausted_for, retry_after, throttle, Throttle, MAX_RETRY_AFTER};
pub use pause::{Observation, Paused, Pauses, Policy};
