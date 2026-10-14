// package scout
//
// import (
// 	"bufio"
// 	"encoding/json"
// 	"fmt"
// 	"os"
// 	"strings"
// 	"testing"
// 	"time"
// )
//
// // The reverse of TestZZWireVectors, for ROLLBACK: whatever the Rust port mints (a sealed config segment, a
// // play ticket, a play token) the Go service must open, to the same fields. Run, with the path of
// // rust_wire_vectors.tsv:
// //
// //	RUST_VECTORS=/abs/path/rust_wire_vectors.tsv go test ./internal/scout -run TestZZRustVectors -v -count=1
// //
// // Each line is `VEC<TAB>KIND<TAB>NAME<TAB>VALUE<TAB>EXPECTED-JSON`, the expectation being what the Rust code
// // decoded its own value to, in the JSON shape Go gives the same struct. The keys are the fixed ones of
// // TestZZWireVectors (zzKey(1) prior, zzKey(100) current), so put that file beside this one.
//
// func TestZZRustVectors(t *testing.T) {
// 	path := os.Getenv("RUST_VECTORS")
// 	if path == "" {
// 		t.Skip("RUST_VECTORS not set")
// 	}
// 	k0, k1 := zzKey(1), zzKey(100)
// 	ringNew, _ := parseSealKeyring(zzB64(k1), zzB64(k0))
// 	tkNew := newTicketKeys(ringNew)
// 	var now time.Time
// 	type tOut struct {
// 		Ok     bool
// 		Err    string
// 		Config *Config
// 		Target *PlayTarget
// 	}
// 	f, err := os.Open(path)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer f.Close()
// 	sc := bufio.NewScanner(f)
// 	sc.Buffer(make([]byte, 1<<20), 1<<20)
// 	counts := map[string]int{}
// 	var lines [][]string
// 	for sc.Scan() {
// 		p := strings.SplitN(sc.Text(), "\t", 5)
// 		if p[0] == "KEY" && p[1] == "now" {
// 			var s int64
// 			fmt.Sscan(p[2], &s)
// 			now = time.Unix(s, 0)
// 		}
// 		if p[0] == "VEC" && len(p) == 5 {
// 			lines = append(lines, p)
// 		}
// 	}
// 	normalise := func(s string) string { // compare as JSON values, not as spellings
// 		var v any
// 		if err := json.Unmarshal([]byte(s), &v); err != nil {
// 			return "unparsable:" + s
// 		}
// 		return zzJSON(v)
// 	}
// 	for _, p := range lines {
// 		kind, name, value, want := p[1], p[2], p[3], normalise(p[4])
// 		var got string
// 		switch kind {
// 		case "sealed_current", "sealed_prior":
// 			c, ok := decodeConfig(ringNew, value)
// 			got = normalise(zzCfgExpect(c, ok))
// 		case "play_token":
// 			tg, ok := decodePlayToken(value)
// 			if !ok {
// 				got = "null"
// 			} else {
// 				got = normalise(zzJSON(tg))
// 				if encodePlayToken(*tg) != value { // Go would mint the very same token
// 					t.Errorf("%s/%s: Go re-encodes to %s, Rust minted %s", kind, name, encodePlayToken(*tg), value)
// 				}
// 			}
// 		case "ticket":
// 			c, tg, err := tkNew.open(value, now)
// 			o := tOut{Ok: err == nil}
// 			if err != nil {
// 				o.Err = err.Error()
// 			} else {
// 				o.Config, o.Target = c, tg
// 			}
// 			got = normalise(zzJSON(o))
// 		default:
// 			t.Fatalf("unknown kind %q", kind)
// 		}
// 		counts[kind]++
// 		if got != want {
// 			t.Errorf("MISMATCH %s/%s\n  rust: %s\n  go:   %s", kind, name, want, got)
// 		}
// 	}
// 	fmt.Printf("RUST->GO checked %v\n", counts)
// }

// What Rust mints, Go must open: this is the rollback direction. `rust_wire_vectors.tsv` holds values the Rust
// code minted with the fixed keys of `go_wire_vectors.tsv`, and the fields Rust itself decoded them to. The
// Go test above opens them with Go's real code (a temporary worktree of den-scout's origin/main, the file
// copied in as `internal/scout/zz_rust_vectors_test.go` beside the generator of `go_wire_vectors.rs`) and
// compares field for field. These tests keep the file honest from this side: every value still opens in Rust
// to its recorded fields, and the fields are the ones that were put in.
//
// Regenerate (nonces are random, so the values change; the expectations do not):
// `cargo test -p den-scout regenerate_rust_wire_vectors -- --ignored`, then run the Go test again.

use std::fmt::Write as _;
use std::time::Duration;

use serde_json::{json, Value};

use super::go_wire_vectors::{
    decoded_config_json, keys, now, open_ticket, ring_old, ring_rotated, target_json,
};
use crate::config::{decode_config, Config, DebridAccount, DebridService};
use crate::play::{decode_play_token, encode_play_token, PlayTarget};
use crate::seal::{seal, SEALED_VERSION};
use crate::ticket::TicketKeys;
use crate::util::b64url_encode;

const VECTORS: &str = include_str!("rust_wire_vectors.tsv");
const IID: &str = "AAECAwQFBgcICQoLDA0ODw";
const H40: &str = "0123456789abcdef0123456789abcdef01234567";
const H32: &str = "abcdefghijklmnopqrstuvwxyz234567";

/// The plaintext configs the installer page would seal: the valid ones of the Go generator's set.
const CONFIGS: [(&str, &str); 7] = [
    ("minimal", r#"{"debrid":[{"service":"torbox","token":"tb-token-1"}]}"#),
    (
        "full",
        r#"{"debrid":[{"service":"realdebrid","token":"rd-1"},{"service":"premiumize","token":"pm-1"},{"service":"torbox","token":"tb-1"}],"indexers":["torrentio","comet","mediafusion","torrentio"],"filters":{"excludeCam":false,"resolutions":["1080p","2160p","1080p","8k"],"preferResolution":"1080p","hdrOnly":true,"minSeeders":12.4,"maxSizeGB":40.6,"excludeRegex":"hindi|dubbed"},"cachedOnly":true,"resultCap":33.5}"#,
    ),
    (
        "availability_iid",
        r#"{"debrid":[{"service":"torbox","token":"tb-av"}],"scope":"availability","iid":"AAECAwQFBgcICQoLDA0ODw","ep":3}"#,
    ),
    (
        "epoch_iid",
        r#"{"debrid":[{"service":"realdebrid","token":"rd-ep"}],"iid":"AAECAwQFBgcICQoLDA0ODw","ep":2147483647}"#,
    ),
    ("quarantined_torz", r#"{"debrid":[{"service":"torbox","token":"tb-q"}],"indexers":["torz","torrentio"]}"#),
    (
        "unicode_token",
        "{\"debrid\":[{\"service\":\"torbox\",\"token\":\"tb-é-日本-é\"}],\"filters\":{\"excludeRegex\":\"日本|é\"}}",
    ),
    (
        "sources",
        r#"{"debrid":[{"service":"torbox","token":"tb-s"}],"sources":["https://addon.example/abc/manifest.json","https://addon.example/abc"]}"#,
    ),
];

struct Line {
    kind: &'static str,
    name: String,
    value: String,
    expect: Value,
}

fn account(service: DebridService, token: &str) -> DebridAccount {
    DebridAccount { service, token: token.to_owned() }
}

fn sealed_segment(recipient: &[u8; 32], plaintext: &str) -> String {
    let mut body = vec![SEALED_VERSION];
    body.extend(seal(recipient, plaintext.as_bytes()).unwrap());
    b64url_encode(&body)
}

fn int(v: i64) -> Option<i64> {
    Some(v)
}

/// Mints every vector, with the expectation Rust itself decodes each to.
fn mint() -> Vec<Line> {
    let ring = ring_rotated();
    let (current, prior) = (&ring.keys[0].pub_key, &ring.keys[1].pub_key);
    let mut out = Vec::new();

    for (name, json) in CONFIGS {
        let seg = sealed_segment(current, json);
        let expect = decoded_config_json(decode_config(Some(&ring), &seg));
        assert_ne!(expect, Value::Null, "{name} must open in Rust");
        out.push(Line { kind: "sealed_current", name: name.to_owned(), value: seg, expect });
    }
    for (name, json) in &CONFIGS[..3] {
        let seg = sealed_segment(prior, json);
        let expect = decoded_config_json(decode_config(Some(&ring), &seg));
        assert_ne!(expect, Value::Null, "{name} must open in Rust");
        out.push(Line { kind: "sealed_prior", name: (*name).to_owned(), value: seg, expect });
    }

    let target = |info_hash: &str, f, s, e| PlayTarget {
        info_hash: info_hash.to_owned(),
        file_idx: f,
        season: s,
        episode: e,
        ..Default::default()
    };
    for (name, t) in [
        ("hash_only", target(H40, None, None, None)),
        ("file", target(H40, int(2), None, None)),
        ("file_zero", target(H40, int(0), None, None)),
        ("episode", target(H32, None, int(1), int(2))),
        ("all", target(H40, int(2), int(1), int(2))),
    ] {
        let tok = encode_play_token(&t);
        let back = decode_play_token(&tok).expect("Rust opens its own token");
        assert_eq!(back, t, "{name}");
        out.push(Line { kind: "play_token", name: name.to_owned(), value: tok, expect: target_json(&back) });
    }

    let rotated = TicketKeys::new(Some(&ring)).unwrap();
    let old = TicketKeys::new(Some(&ring_old())).unwrap();
    let exp = now() + Duration::from_secs(3600);
    let accounts = [account(DebridService::TorBox, "tb-tok"), account(DebridService::RealDebrid, "rd-tok")];
    let cfg = |n: usize, iid: &str, epoch: i64, scope: &str| Config {
        debrid: accounts[..n].to_vec(),
        iid: iid.to_owned(),
        epoch,
        scope: scope.to_owned(),
        ..Default::default()
    };
    let t = |info_hash: &str| target(info_hash, None, None, None);
    let mints: Vec<(&str, &TicketKeys, Config, PlayTarget)> = vec![
        ("bare", &rotated, cfg(1, "", 0, ""), t(H40)),
        (
            "full",
            &rotated,
            cfg(2, IID, 7, ""),
            PlayTarget {
                info_hash: H40.to_owned(),
                file_idx: int(2),
                season: int(1),
                episode: int(2),
                release_size: 8_589_934_592,
                title: "Some.Movie.2160p.REMUX-GRP".to_owned(),
                imdb: "tt0111161".to_owned(),
            },
        ),
        ("availability_scope", &rotated, cfg(1, "", 0, "availability"), t(H40)),
        ("iid_only", &rotated, cfg(1, IID, 0, ""), t(H32)),
        ("epoch_only", &rotated, cfg(1, "", 2, ""), t(H40)),
        ("file_zero", &rotated, cfg(1, "", 0, ""), target(H40, int(0), None, None)),
        ("size_only", &rotated, cfg(1, "", 0, ""), PlayTarget { release_size: 1, ..t(H40) }),
        (
            "title_only",
            &rotated,
            cfg(1, "", 0, ""),
            PlayTarget { title: "Ünï \"quoted\" <b>&".to_owned(), ..t(H40) },
        ),
        ("imdb_only", &rotated, cfg(1, "", 0, ""), PlayTarget { imdb: "tt7".to_owned(), ..t(H40) }),
        ("episode", &rotated, cfg(2, IID, 0, ""), target(H40, None, int(2), int(1))),
        ("two_accounts_no_extras", &rotated, cfg(2, "", 0, ""), t(H32)),
        ("prior_key", &old, cfg(2, IID, 1, ""), target(H40, int(1), None, None)),
    ];
    for (name, keys, config, target) in mints {
        let tok = keys.mint(&config, &target, exp);
        let expect = open_ticket(&rotated, &tok);
        // The fields that went in are the fields that come out, in Rust, before Go is asked.
        assert_eq!(expect["Ok"], true, "{name}");
        assert_eq!(expect["Target"], target_json(&target), "{name}: target");
        assert_eq!(expect["Config"]["IID"], config.iid, "{name}: iid");
        assert_eq!(expect["Config"]["Epoch"], config.epoch, "{name}: epoch");
        assert_eq!(expect["Config"]["Scope"], config.scope, "{name}: scope");
        assert_eq!(expect["Config"]["Debrid"].as_array().map_or(0, Vec::len), config.debrid.len(), "{name}");
        out.push(Line { kind: "ticket", name: name.to_owned(), value: tok, expect });
    }
    out
}

fn render(lines: &[Line]) -> String {
    let k = keys();
    let mut s = String::new();
    for name in ["current", "prior", "now"] {
        writeln!(s, "KEY\t{name}\t{}", k[name]).unwrap();
    }
    for l in lines {
        writeln!(s, "VEC\t{}\t{}\t{}\t{}", l.kind, l.name, l.value, l.expect).unwrap();
    }
    s
}

#[test]
#[ignore = "rewrites rust_wire_vectors.tsv with freshly minted (random-nonce) values"]
fn regenerate_rust_wire_vectors() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/src/tests/rust_wire_vectors.tsv");
    std::fs::write(path, render(&mint())).unwrap();
}

/// Minting stays sound: every kind of vector the file holds is what `mint` makes today.
#[test]
fn the_vector_file_covers_what_rust_mints() {
    let kinds = |text: &str| {
        let mut v: Vec<(String, String)> = text
            .lines()
            .filter_map(|l| l.strip_prefix("VEC\t"))
            .map(|l| {
                let f: Vec<&str> = l.splitn(4, '\t').collect();
                (f[0].to_owned(), f[1].to_owned())
            })
            .collect();
        v.sort();
        v
    };
    assert_eq!(kinds(VECTORS), kinds(&render(&mint())));
}

/// Each committed value still opens in Rust to the fields recorded beside it, which is what Go was held to.
#[test]
fn committed_rust_vectors_open_in_rust_to_their_recorded_fields() {
    let ring = ring_rotated();
    let rotated = TicketKeys::new(Some(&ring)).unwrap();
    let mut counts = std::collections::BTreeMap::new();
    for l in VECTORS.lines().filter_map(|l| l.strip_prefix("VEC\t")) {
        let f: Vec<&str> = l.splitn(4, '\t').collect();
        let (kind, name, value) = (f[0], f[1], f[2]);
        let expect: Value = serde_json::from_str(f[3]).unwrap();
        let got = match kind {
            "sealed_current" | "sealed_prior" => decoded_config_json(decode_config(Some(&ring), value)),
            "play_token" => decode_play_token(value).map_or(Value::Null, |t| target_json(&t)),
            "ticket" => open_ticket(&rotated, value),
            other => panic!("unknown kind {other}"),
        };
        assert_eq!(got, expect, "{kind}/{name}");
        *counts.entry(kind).or_insert(0) += 1;
    }
    assert_eq!(json!(counts), json!({"play_token": 5, "sealed_current": 7, "sealed_prior": 3, "ticket": 12}));
}
