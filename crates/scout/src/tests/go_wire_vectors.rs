// package scout
//
// import (
// 	"encoding/base64"
// 	"encoding/json"
// 	"fmt"
// 	"os"
// 	"strings"
// 	"testing"
// 	"time"
// )
//
// // Wire-format vectors for the Rust port. Run:
// //
// //	go test ./internal/scout -run TestZZWireVectors -v
// //
// // Every line is `VEC<TAB>KIND<TAB>NAME<TAB>VALUE<TAB>EXPECTED-JSON`; the expectation is whatever THIS Go code
// // decodes the value to, never hand-written. Keys are fixed; sealing and ticket nonces are random.
//
// func zzKey(seed byte) [32]byte {
// 	var k [32]byte
// 	for i := range k {
// 		k[i] = seed + byte(i)*3
// 	}
// 	return k
// }
//
// func zzB64(k [32]byte) string { return base64.StdEncoding.EncodeToString(k[:]) }
//
// func zzJSON(v any) string {
// 	b, err := json.Marshal(v)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return string(b)
// }
//
// func zzEmit(kind, name, value, expect string) {
// 	fmt.Fprintf(os.Stdout, "VEC\t%s\t%s\t%s\t%s\n", kind, name, value, expect)
// }
//
// type zzCfgOut struct {
// 	Config      *Config
// 	Quarantined []Indexer
// }
//
// func zzCfgExpect(c *Config, ok bool) string {
// 	if !ok {
// 		return "null"
// 	}
// 	return zzJSON(zzCfgOut{Config: c, Quarantined: c.Quarantined})
// }
//
// const zzIID = "AAECAwQFBgcICQoLDA0ODw" // 16 bytes 00..0f, canonical
//
// func zzSealSegment(pub [32]byte, plaintext string) string {
// 	ct, err := seal(&pub, []byte(plaintext))
// 	if err != nil {
// 		panic(err)
// 	}
// 	return b64urlEncode(append([]byte{sealedVersion}, ct...))
// }
//
// func TestZZWireVectors(t *testing.T) {
// 	k0, k1 := zzKey(1), zzKey(100)
// 	ringOld, _ := parseSealKeyring(zzB64(k0), "")
// 	ringNew, _ := parseSealKeyring(zzB64(k1), zzB64(k0)+", not-a-key")
// 	ringOther, _ := parseSealKeyring(zzB64(zzKey(200)), "")
// 	fmt.Printf("KEY\tcurrent\t%s\n", zzB64(k1))
// 	fmt.Printf("KEY\tprior\t%s\n", zzB64(k0))
// 	fmt.Printf("KEY\tpub_current\t%s\n", ringNew.currentPubBase64())
// 	fmt.Printf("KEY\tpub_prior\t%s\n", ringOld.currentPubBase64())
//
// 	// ---- config path segments -------------------------------------------------------------------------
// 	cfgs := []struct{ name, json string }{
// 		{"minimal", `{"debrid":[{"service":"torbox","token":"tb-token-1"}]}`},
// 		{"full", `{"debrid":[{"service":"realdebrid","token":"rd-1"},{"service":"premiumize","token":"pm-1"},{"service":"torbox","token":"tb-1"}],` +
// 			`"indexers":["torrentio","comet","mediafusion","torrentio"],"filters":{"excludeCam":false,"resolutions":["1080p","2160p","1080p","8k"],` +
// 			`"preferResolution":"1080p","hdrOnly":true,"minSeeders":12.4,"maxSizeGB":40.6,"excludeRegex":"hindi|dubbed"},` +
// 			`"cachedOnly":true,"resultCap":33.5}`},
// 		{"availability_iid", `{"debrid":[{"service":"torbox","token":"tb-av"}],"scope":"availability","iid":"` + zzIID + `","ep":3}`},
// 		{"epoch_iid", `{"debrid":[{"service":"realdebrid","token":"rd-ep"}],"iid":"` + zzIID + `","ep":2147483647}`},
// 		{"quarantined_torz", `{"debrid":[{"service":"torbox","token":"tb-q"}],"indexers":["torz","torrentio"]}`},
// 		{"only_torz_defaults", `{"debrid":[{"service":"torbox","token":"tb-q2"}],"indexers":["torz"]}`},
// 		{"counted_repeat_dropped", `{"debrid":[{"service":"torbox","token":"tb-r"}],"filters":{"excludeRegex":"S\\d{2}E\\d{2}"}}`},
// 		{"drops_bad_accounts", `{"debrid":[{"service":"nope","token":"x"},{"service":"torbox","token":""},{"service":"torbox","token":"tb-ok"}]}`},
// 		{"resultcap_clamped", `{"debrid":[{"service":"torbox","token":"tb-c"}],"resultCap":9999,"filters":{"minSeeders":-4,"maxSizeGB":5000}}`},
// 		{"unicode_token", `{"debrid":[{"service":"torbox","token":"tb-é-日本-é"}],"filters":{"excludeRegex":"日本|é"}}`},
// 		{"sources", `{"debrid":[{"service":"torbox","token":"tb-s"}],"sources":["https://addon.example/abc/manifest.json","https://addon.example/abc"]}`},
// 		{"mixed_case_keys", `{"DEBRID":[{"Service":"torbox","TOKEN":"tb-case"}],"CachedOnly":true,"Filters":{"HDROnly":true}}`},
// 		{"bad_scope", `{"debrid":[{"service":"torbox","token":"tb"}],"scope":"admin"}`},
// 		{"bad_iid", `{"debrid":[{"service":"torbox","token":"tb"}],"iid":"short"}`},
// 		{"noncanonical_iid", `{"debrid":[{"service":"torbox","token":"tb"}],"iid":"AAECAwQFBgcICQoLDA0ODx"}`},
// 		{"epoch_too_big", `{"debrid":[{"service":"torbox","token":"tb"}],"ep":2147483648}`},
// 		{"no_accounts", `{"indexers":["torrentio"]}`},
// 	}
// 	for _, c := range cfgs {
// 		seg := zzSealSegment(ringNew.keys[0].pub, c.json)
// 		got, ok := decodeConfig(ringNew, seg)
// 		zzEmit("sealed_current", c.name, seg, zzCfgExpect(got, ok))
// 	}
// 	// sealed to the PRIOR key, opened through the rotated ring
// 	for _, c := range cfgs[:3] {
// 		seg := zzSealSegment(ringNew.keys[1].pub, c.json)
// 		got, ok := decodeConfig(ringNew, seg)
// 		zzEmit("sealed_prior", c.name, seg, zzCfgExpect(got, ok))
// 	}
// 	// padded / standard-alphabet-free variants of the segment
// 	{
// 		seg := zzSealSegment(ringNew.keys[0].pub, cfgs[0].json)
// 		padded := seg + strings.Repeat("=", (4-len(seg)%4)%4)
// 		got, ok := decodeConfig(ringNew, padded)
// 		zzEmit("sealed_current", "padded_segment", padded, zzCfgExpect(got, ok))
// 	}
// 	// sealed under a key the ring does not hold, and with the version byte but a short body
// 	{
// 		seg := zzSealSegment(ringOther.keys[0].pub, cfgs[0].json)
// 		got, ok := decodeConfig(ringNew, seg)
// 		zzEmit("sealed_current", "unknown_key", seg, zzCfgExpect(got, ok))
// 		short := b64urlEncode([]byte{sealedVersion, 1, 2, 3})
// 		got, ok = decodeConfig(ringNew, short)
// 		zzEmit("sealed_current", "too_short", short, zzCfgExpect(got, ok))
// 	}
// 	// plaintext segments: valid only with NO keyring
// 	for _, c := range cfgs[:8] {
// 		seg := b64urlEncode([]byte(c.json))
// 		got, ok := decodeConfig(nil, seg)
// 		zzEmit("plain_nokey", c.name, seg, zzCfgExpect(got, ok))
// 	}
// 	{
// 		seg := b64urlEncode([]byte(cfgs[0].json))
// 		got, ok := decodeConfig(ringNew, seg)
// 		zzEmit("plain_withkey", "refused", seg, zzCfgExpect(got, ok))
// 		sc := b64urlEncode([]byte(cfgs[2].json))
// 		got, ok = decodeConfig(nil, sc)
// 		zzEmit("plain_nokey", "scope_refused", sc, zzCfgExpect(got, ok))
// 		sealedNoKey := zzSealSegment(ringNew.keys[0].pub, cfgs[0].json)
// 		got, ok = decodeConfig(nil, sealedNoKey)
// 		zzEmit("sealed_nokey", "refused", sealedNoKey, zzCfgExpect(got, ok))
// 		pad := seg + strings.Repeat("=", (4-len(seg)%4)%4)
// 		got, ok = decodeConfig(nil, pad)
// 		zzEmit("plain_nokey", "padded_segment", pad, zzCfgExpect(got, ok))
// 	}
//
// 	// ---- play tokens -----------------------------------------------------------------------------------
// 	h40 := "0123456789abcdef0123456789abcdef01234567"
// 	h32 := "abcdefghijklmnopqrstuvwxyz234567"
// 	one, two, zero := 1, 2, 0
// 	enc := []struct {
// 		name string
// 		t    PlayTarget
// 	}{
// 		{"hash_only", PlayTarget{InfoHash: h40}},
// 		{"file", PlayTarget{InfoHash: h40, FileIdx: &two}},
// 		{"file_zero", PlayTarget{InfoHash: h40, FileIdx: &zero}},
// 		{"episode", PlayTarget{InfoHash: h32, Season: &one, Episode: &two}},
// 		{"all", PlayTarget{InfoHash: h40, FileIdx: &two, Season: &one, Episode: &two}},
// 	}
// 	for _, e := range enc {
// 		tok := encodePlayToken(e.t)
// 		got, ok := decodePlayToken(tok)
// 		var exp string
// 		if ok {
// 			exp = zzJSON(got)
// 		} else {
// 			exp = "null"
// 		}
// 		zzEmit("play_token", e.name, tok, exp)
// 	}
// 	raws := []struct{ name, json string }{
// 		{"uppercase_hash", `{"h":"` + strings.ToUpper(h40) + `","f":3}`},
// 		{"fractional_dropped", `{"h":"` + h40 + `","f":1.5,"s":2.0,"e":-1}`},
// 		{"float_integral", `{"h":"` + h40 + `","f":3.0,"s":1e1}`},
// 		{"null_fields", `{"h":"` + h40 + `","f":null,"s":null}`},
// 		{"upper_keys", `{"H":"` + h40 + `","F":4,"S":1,"E":2}`},
// 		{"extra_fields", `{"h":"` + h40 + `","f":4,"x":9,"title":"y"}`},
// 		{"short_hash", `{"h":"abc"}`},
// 		{"bad_chars", `{"h":"` + strings.Repeat("-", 40) + `"}`},
// 		{"missing_hash", `{"f":1}`},
// 		{"not_json", `hello`},
// 		{"string_f", `{"h":"` + h40 + `","f":"1"}`},
// 	}
// 	for _, r := range raws {
// 		tok := b64urlEncode([]byte(r.json))
// 		got, ok := decodePlayToken(tok)
// 		var exp string
// 		if ok {
// 			exp = zzJSON(got)
// 		} else {
// 			exp = "null"
// 		}
// 		zzEmit("play_token", r.name, tok, exp)
// 	}
//
// 	// ---- tickets ----------------------------------------------------------------------------------------
// 	now := time.Unix(1_800_000_000, 0)
// 	fmt.Printf("KEY\tnow\t%d\n", now.Unix())
// 	tkNew, tkOld := newTicketKeys(ringNew), newTicketKeys(ringOld)
// 	type tOut struct {
// 		Ok     bool
// 		Err    string
// 		Config *Config
// 		Target *PlayTarget
// 	}
// 	openExpect := func(tk *ticketKeys, tok string) string {
// 		c, tg, err := tk.open(tok, now)
// 		o := tOut{Ok: err == nil}
// 		if err != nil {
// 			o.Err = err.Error()
// 		} else {
// 			o.Config, o.Target = c, tg
// 		}
// 		return zzJSON(o)
// 	}
// 	exp := now.Add(time.Hour)
// 	accounts := []DebridAccount{{Service: "torbox", Token: "tb-tok"}, {Service: "realdebrid", Token: "rd-tok"}}
// 	mints := []struct {
// 		name string
// 		tk   *ticketKeys
// 		cfg  *Config
// 		tgt  PlayTarget
// 	}{
// 		{"bare", tkNew, &Config{Debrid: accounts[:1]}, PlayTarget{InfoHash: h40}},
// 		{"full", tkNew, &Config{Debrid: accounts, IID: zzIID, Epoch: 7}, PlayTarget{InfoHash: h40, FileIdx: &two, Season: &one, Episode: &two, ReleaseSize: 8_589_934_592, Title: "Some.Movie.2160p.REMUX-GRP", IMDb: "tt0111161"}},
// 		{"availability_scope", tkNew, &Config{Debrid: accounts[:1], Scope: scopeAvailability}, PlayTarget{InfoHash: h40}},
// 		{"iid_only", tkNew, &Config{Debrid: accounts[:1], IID: zzIID}, PlayTarget{InfoHash: h32}},
// 		{"epoch_only", tkNew, &Config{Debrid: accounts[:1], Epoch: 2}, PlayTarget{InfoHash: h40}},
// 		{"file_zero", tkNew, &Config{Debrid: accounts[:1]}, PlayTarget{InfoHash: h40, FileIdx: &zero}},
// 		{"size_only", tkNew, &Config{Debrid: accounts[:1]}, PlayTarget{InfoHash: h40, ReleaseSize: 1}},
// 		{"title_only", tkNew, &Config{Debrid: accounts[:1]}, PlayTarget{InfoHash: h40, Title: "Ünï \"quoted\" <b>&"}},
// 		{"imdb_only", tkNew, &Config{Debrid: accounts[:1]}, PlayTarget{InfoHash: h40, IMDb: "tt7"}},
// 		{"episode", tkNew, &Config{Debrid: accounts, IID: zzIID}, PlayTarget{InfoHash: h40, Season: &two, Episode: &one}},
// 		{"prior_key", tkOld, &Config{Debrid: accounts, IID: zzIID, Epoch: 1}, PlayTarget{InfoHash: h40, FileIdx: &one}},
// 	}
// 	for _, m := range mints {
// 		tok := m.tk.mint(m.cfg, m.tgt, exp)
// 		zzEmit("ticket", m.name, tok, openExpect(tkNew, tok))
// 	}
// 	// a ticket minted today under the rotated ring's current key must NOT open under the old-only ring
// 	{
// 		tok := tkNew.mint(&Config{Debrid: accounts[:1]}, PlayTarget{InfoHash: h40}, exp)
// 		zzEmit("ticket", "current_key_not_in_old_ring", tok, openExpect(tkOld, tok))
// 	}
//
// 	// legacy / hand-built payloads sealed with the ring's current play key
// 	sealRaw := func(tk *ticketKeys, payload string) string {
// 		nonce := make([]byte, 24, 24+len(payload)+16)
// 		for i := range nonce {
// 			nonce[i] = byte(i*5 + 3)
// 		}
// 		return b64urlEncode(tk.aeads[0].Seal(nonce, nonce, []byte(payload), nil))
// 	}
// 	x := fmt.Sprint(exp.Unix())
// 	acct := `[["torbox","tb-tok"]]`
// 	legacy := []struct{ name, payload string }{
// 		{"pre_iid_pre_size", `{"h":"` + h40 + `","d":` + acct + `,"x":` + x + `}`},
// 		{"pre_size_with_iid", `{"h":"` + h40 + `","f":2,"d":` + acct + `,"x":` + x + `,"i":"` + zzIID + `","p":4}`},
// 		{"pre_title", `{"h":"` + h40 + `","d":` + acct + `,"x":` + x + `,"i":"` + zzIID + `","z":123456}`},
// 		{"explicit_zeros", `{"h":"` + h40 + `","f":0,"s":0,"e":0,"d":` + acct + `,"x":` + x + `,"p":0,"z":0,"i":"","scope":"","rt":"","tt":""}`},
// 		{"uppercase_hash", `{"h":"` + strings.ToUpper(h40) + `","d":` + acct + `,"x":` + x + `}`},
// 		{"negative_size", `{"h":"` + h40 + `","d":` + acct + `,"x":` + x + `,"z":-5}`},
// 		{"uppercase_keys", `{"H":"` + h40 + `","D":` + acct + `,"X":` + x + `,"I":"` + zzIID + `","Z":9}`},
// 		{"unknown_fields", `{"h":"` + h40 + `","d":` + acct + `,"x":` + x + `,"future":{"a":1},"cap":3}`},
// 		{"scope_availability", `{"h":"` + h40 + `","d":` + acct + `,"x":` + x + `,"scope":"availability"}`},
// 		{"unknown_scope", `{"h":"` + h40 + `","d":` + acct + `,"x":` + x + `,"scope":"admin"}`},
// 		{"expired", `{"h":"` + h40 + `","d":` + acct + `,"x":` + fmt.Sprint(now.Unix()-1) + `}`},
// 		{"expires_exactly_now", `{"h":"` + h40 + `","d":` + acct + `,"x":` + fmt.Sprint(now.Unix()) + `}`},
// 		{"one_second_left", `{"h":"` + h40 + `","d":` + acct + `,"x":` + fmt.Sprint(now.Unix()+1) + `}`},
// 		{"unknown_service", `{"h":"` + h40 + `","d":[["alldebrid","t"]],"x":` + x + `}`},
// 		{"empty_token", `{"h":"` + h40 + `","d":[["torbox",""]],"x":` + x + `}`},
// 		{"no_accounts", `{"h":"` + h40 + `","d":[],"x":` + x + `}`},
// 		{"missing_accounts", `{"h":"` + h40 + `","x":` + x + `}`},
// 		{"short_hash", `{"h":"abc","d":` + acct + `,"x":` + x + `}`},
// 		{"not_json", `nope`},
// 		{"fractional_size_rejected", `{"h":"` + h40 + `","d":` + acct + `,"x":` + x + `,"z":1.5}`},
// 	}
// 	for _, l := range legacy {
// 		tok := sealRaw(tkNew, l.payload)
// 		zzEmit("ticket", "payload_"+l.name, tok, openExpect(tkNew, tok))
// 	}
// 	// the same payload sealed under the PRIOR key's play key
// 	{
// 		tok := sealRaw(tkOld, legacy[1].payload)
// 		zzEmit("ticket", "payload_prior_key", tok, openExpect(tkNew, tok))
// 	}
// 	// transport variants
// 	{
// 		tok := tkNew.mint(&Config{Debrid: accounts[:1]}, PlayTarget{InfoHash: h40}, exp)
// 		pad := tok + strings.Repeat("=", (4-len(tok)%4)%4)
// 		zzEmit("ticket", "padded", pad, openExpect(tkNew, pad))
// 		raw, _ := b64urlDecode(tok)
// 		raw[len(raw)-1] ^= 1
// 		bad := b64urlEncode(raw)
// 		zzEmit("ticket", "tampered", bad, openExpect(tkNew, bad))
// 		short := b64urlEncode(raw[:30])
// 		zzEmit("ticket", "too_short", short, openExpect(tkNew, short))
// 		zzEmit("ticket", "not_base64", "!!!!", openExpect(tkNew, "!!!!"))
// 		huge := strings.Repeat("A", maxConfigBlob+1)
// 		zzEmit("ticket", "over_ceiling", "@A*16385", openExpect(tkNew, huge))
// 	}
// }

// The Rust port must open, byte for byte, what the live Go code produced. `go_wire_vectors.tsv` holds the
// lines the Go generator above printed (`VEC<TAB>kind<TAB>name<TAB>value<TAB>expected-json`, plus `KEY` lines
// for the fixed keys); `expected` is whatever Go itself decoded the value to, so nothing here is hand-written.
//
// Regenerate: put the Go file above at `internal/scout/zz_vectors_test.go` in a den-scout checkout, run
// `go test ./internal/scout -run TestZZWireVectors -v -count=1`, and keep the `VEC` / `KEY` lines. Sealing and
// ticket nonces are random, so a regeneration changes the values but never the expectations.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

use crate::config::{decode_config, Config};
use crate::play::{decode_play_token, encode_play_token, PlayTarget};
use crate::seal::{parse_seal_keyring, SealKeyring};
use crate::ticket::{TicketError, TicketKeys};

const VECTORS: &str = include_str!("go_wire_vectors.tsv");

struct Vector {
    kind: &'static str,
    name: &'static str,
    value: String,
    expect: Value,
}

fn keys() -> BTreeMap<&'static str, &'static str> {
    VECTORS
        .lines()
        .filter_map(|l| l.strip_prefix("KEY\t"))
        .map(|l| l.split_once('\t').expect("KEY lines are name<TAB>value"))
        .collect()
}

fn vectors(kind: &str) -> Vec<Vector> {
    let out: Vec<Vector> = VECTORS
        .lines()
        .filter(|l| l.starts_with("VEC\t"))
        .map(|l| {
            let f: Vec<&'static str> = l.splitn(5, '\t').collect();
            assert_eq!(f.len(), 5, "malformed vector line: {l}");
            let value = match f[3].strip_prefix("@A*") {
                Some(n) => "A".repeat(n.parse().unwrap()),
                None => f[3].to_owned(),
            };
            Vector { kind: f[1], name: f[2], value, expect: serde_json::from_str(f[4]).expect("expected is JSON") }
        })
        .filter(|v| v.kind == kind)
        .collect();
    assert!(!out.is_empty(), "no vectors of kind {kind}");
    out
}

/// The rotated ring Go decoded with: current key first, the prior key after it, and a junk entry the
/// parser must skip.
fn ring_rotated() -> SealKeyring {
    let k = keys();
    parse_seal_keyring(k["current"], &format!("{}, not-a-key", k["prior"])).unwrap().unwrap()
}

/// The ring from before the rotation: only the prior key.
fn ring_old() -> SealKeyring {
    parse_seal_keyring(keys()["prior"], "").unwrap().unwrap()
}

fn now() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(keys()["now"].parse().unwrap())
}

fn or_null<T: Clone + serde::Serialize>(xs: &[T]) -> Value {
    if xs.is_empty() {
        Value::Null
    } else {
        json!(xs)
    }
}

/// A Rust `Config` in the JSON shape Go's `json.Marshal` gave the Go one (no tags, so Go field names; a nil
/// slice is `null`).
fn config_json(c: &Config) -> Value {
    json!({
        "Debrid": or_null(&c.debrid.iter().map(|d| json!({"Service": d.service.as_str(), "Token": d.token})).collect::<Vec<_>>()),
        "Indexers": or_null(&c.indexers.iter().map(|i| i.0.clone()).collect::<Vec<_>>()),
        "Sources": or_null(&c.sources),
        "Filters": {
            "ExcludeCam": c.filters.exclude_cam,
            "Resolutions": or_null(&c.filters.resolutions),
            "PreferResolution": c.filters.prefer_resolution,
            "HDROnly": c.filters.hdr_only,
            "MinSeeders": c.filters.min_seeders,
            "MaxSizeGB": c.filters.max_size_gb,
            "ExcludeRegex": c.filters.exclude_regex,
        },
        "CachedOnly": c.cached_only,
        "ResultCap": c.result_cap,
        "Scope": c.scope,
        "IID": c.iid,
        "Epoch": c.epoch,
    })
}

fn target_json(t: &PlayTarget) -> Value {
    json!({
        "InfoHash": t.info_hash, "FileIdx": t.file_idx, "Season": t.season, "Episode": t.episode,
        "ReleaseSize": t.release_size, "Title": t.title, "IMDb": t.imdb,
    })
}

/// What the Go generator printed for a config: `{"Config":..,"Quarantined":..}` or `null`.
fn decoded_config_json(c: Option<Config>) -> Value {
    match c {
        None => Value::Null,
        Some(c) => json!({
            "Config": config_json(&c),
            "Quarantined": or_null(&c.quarantined.iter().map(|i| i.0.clone()).collect::<Vec<_>>()),
        }),
    }
}

fn check_configs(kind: &str, kr: Option<&SealKeyring>) -> usize {
    let vs = vectors(kind);
    for v in &vs {
        let got = decoded_config_json(decode_config(kr, &v.value));
        assert_eq!(got, v.expect, "config vector {}/{}", v.kind, v.name);
    }
    vs.len()
}

#[test]
fn sealed_configs_sealed_to_the_current_key_open_as_go_opened_them() {
    let ring = ring_rotated();
    assert_eq!(check_configs("sealed_current", Some(&ring)), 20);
}

#[test]
fn sealed_configs_sealed_to_a_prior_key_open_through_the_rotated_ring() {
    let ring = ring_rotated();
    assert_eq!(check_configs("sealed_prior", Some(&ring)), 3);
    // ... and are unreadable once the prior key leaves the ring.
    let k = keys();
    let current_only = parse_seal_keyring(k["current"], "").unwrap().unwrap();
    for v in vectors("sealed_prior") {
        assert!(decode_config(Some(&current_only), &v.value).is_none(), "{}", v.name);
    }
}

#[test]
fn the_pinned_public_keys_match_go() {
    let k = keys();
    assert_eq!(ring_rotated().current_pub_base64(), k["pub_current"]);
    assert_eq!(ring_old().current_pub_base64(), k["pub_prior"]);
}

#[test]
fn sealed_segments_are_refused_without_a_keyring() {
    assert_eq!(check_configs("sealed_nokey", None), 1);
}

#[test]
fn plaintext_config_segments_decode_as_go_decoded_them_without_a_keyring() {
    assert_eq!(check_configs("plain_nokey", None), 10);
}

#[test]
fn plaintext_config_segments_are_refused_when_a_keyring_is_set() {
    let ring = ring_rotated();
    assert_eq!(check_configs("plain_withkey", Some(&ring)), 1);
}

#[test]
fn play_tokens_decode_as_go_decoded_them() {
    let vs = vectors("play_token");
    for v in &vs {
        let got = decode_play_token(&v.value).map_or(Value::Null, |t| target_json(&t));
        assert_eq!(got, v.expect, "play token {}", v.name);
    }
    assert_eq!(vs.len(), 16);
}

#[test]
fn encoded_play_tokens_match_go_byte_for_byte() {
    let mut n = 0;
    for v in vectors("play_token") {
        if !["hash_only", "file", "file_zero", "episode", "all"].contains(&v.name) {
            continue;
        }
        let t = decode_play_token(&v.value).expect("Go minted a valid token");
        assert_eq!(encode_play_token(&t), v.value, "play token {}", v.name);
        n += 1;
    }
    assert_eq!(n, 5);
}

fn open_ticket(tk: &TicketKeys, ticket: &str) -> Value {
    match tk.open(ticket, now()) {
        Ok((c, t)) => json!({
            "Ok": true, "Err": "", "Config": config_json(&c), "Target": target_json(&t),
        }),
        Err(e) => json!({
            "Ok": false,
            "Err": match e {
                TicketError::Bad => "ticket: does not open",
                TicketError::Expired => "ticket: expired",
            },
            "Config": null,
            "Target": null,
        }),
    }
}

#[test]
fn play_tickets_open_as_go_opened_them() {
    let rotated = TicketKeys::new(Some(&ring_rotated())).unwrap();
    let old = TicketKeys::new(Some(&ring_old())).unwrap();
    let vs = vectors("ticket");
    let (mut opened, mut refused) = (0, 0);
    for v in &vs {
        // A ticket minted with the rotated ring's current key is deliberately tried on the pre-rotation ring.
        let tk = if v.name == "current_key_not_in_old_ring" { &old } else { &rotated };
        let got = open_ticket(tk, &v.value);
        assert_eq!(got, v.expect, "ticket {}", v.name);
        if got["Ok"] == true {
            opened += 1;
        } else {
            refused += 1;
        }
    }
    // Guard against a vector set that quietly stops exercising one side.
    assert_eq!((vs.len(), opened, refused), (38, 23, 15));
}

#[test]
fn go_minted_tickets_carry_every_optional_field_through() {
    let rotated = TicketKeys::new(Some(&ring_rotated())).unwrap();
    let full = vectors("ticket").into_iter().find(|v| v.name == "full").unwrap();
    let (c, t) = rotated.open(&full.value, now()).unwrap();
    assert_eq!((c.iid.as_str(), c.epoch, c.debrid.len()), ("AAECAwQFBgcICQoLDA0ODw", 7, 2));
    assert_eq!(
        (t.release_size, t.title.as_str(), t.imdb.as_str(), t.file_idx, t.season, t.episode),
        (8_589_934_592, "Some.Movie.2160p.REMUX-GRP", "tt0111161", Some(2), Some(1), Some(2))
    );
    // A prior-key ticket opens through the rotated ring only because the ring still holds that key.
    let prior = vectors("ticket").into_iter().find(|v| v.name == "prior_key").unwrap();
    assert!(rotated.open(&prior.value, now()).is_ok());
    assert!(TicketKeys::new(Some(&parse_seal_keyring(keys()["current"], "").unwrap().unwrap()))
        .unwrap()
        .open(&prior.value, now())
        .is_err());
}
