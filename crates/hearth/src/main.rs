//! den-hearth — Den's self-hosted node: Scout, Reel, Subtitles and Remux in one process.
//!
//!   /scout/...  → den-scout transport and operations
//!   /reel/...   → den-reel      (`/reel/play/<id>.mp4` is Reel's `/play/<id>.mp4`)
//!   /subs/...   → den-subtitles resources and operations
//!   /remux/...  → den-remux     (unchanged: Remux already serves everything under `/remux`)
//!   /den/...    → the combined Den addon: one manifest and config for Scout, Subtitles and Reel (`den`)
//!   /health, /den/health → all four, each as its own service reports it
//!
//! Each service also has a listener of its own on the port its old container used (`ports`), where transport
//! and operational requests keep their path unchanged. Addon installation exists only under `/den`.
//!
//! Each service is the same library its standalone binary runs; hearth supplies the listeners, the
//! runtime and the signal handling, and gives each service its own environment (see `env`).

mod admit;
mod alloc;
mod application;
mod bootstrap;
mod den;
mod env;
mod guest;
mod inproc;
mod media;
mod playback;
mod ports;
mod route;
mod scout;
mod server;
mod services;
mod sock;
mod wake;
mod wanted;

pub(crate) use application::{bad_path, handle, json, with_path, Hearth};
#[cfg(test)]
pub(crate) use application::{Limits, Options};
pub(crate) use den_http::Body;

#[cfg(test)]
use bootstrap::parsed_or;
#[cfg(test)]
use bytes::Bytes;
#[cfg(test)]
use http_body_util::{BodyExt, Empty};
#[cfg(test)]
use hyper::body::Incoming;
#[cfg(test)]
use hyper::service::service_fn;
#[cfg(test)]
use hyper::{Request, Response, StatusCode};
#[cfg(test)]
use hyper_util::rt::TokioIo;
#[cfg(test)]
use route::Service;
#[cfg(test)]
use serde_json::Value;
#[cfg(test)]
use server::{adopt_inherited, listener_for, serve_until};
#[cfg(test)]
use std::convert::Infallible;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;
#[cfg(test)]
use tokio::net::TcpListener;
#[cfg(test)]
use tokio::sync::watch;

fn main() {
    bootstrap::main();
}

#[cfg(test)]
mod tests;
