package scout

// Go counterparts of crates/scout/benches/scout.rs, so the Rust port is compared with the code it replaces on
// identical inputs. NOT part of den-scout: bench/go/run.sh copies this file into a throwaway worktree of
// den-scout and deletes it after.
//
// Inputs come from bench/gen_fixtures.py ($BENCH_FIXTURES) and crates/scout/testdata ($BENCH_TESTDATA).

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fixture(b *testing.B, name string) []byte {
	b.Helper()
	dir := os.Getenv("BENCH_FIXTURES")
	if dir == "" {
		b.Skip("BENCH_FIXTURES is not set")
	}
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		b.Fatal(err)
	}
	return data
}

func testdata(b *testing.B, name string) []byte {
	b.Helper()
	dir := os.Getenv("BENCH_TESTDATA")
	if dir == "" {
		b.Skip("BENCH_TESTDATA is not set")
	}
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		b.Fatal(err)
	}
	return data
}

func releases(b *testing.B, n int) []RawStream {
	b.Helper()
	var raw []struct {
		Title   string `json:"title"`
		Size    *int   `json:"size"`
		Seeders *int   `json:"seeders"`
		Cached  bool   `json:"cached"`
		Hash    string `json:"hash"`
	}
	if err := json.Unmarshal(fixture(b, "releases.json"), &raw); err != nil {
		b.Fatal(err)
	}
	out := make([]RawStream, 0, n)
	for _, r := range raw[:n] {
		out = append(out, RawStream{InfoHash: r.Hash, Title: r.Title, SizeBytes: r.Size, Seeders: r.Seeders,
			Cached: r.Cached, CacheKnown: true, Source: "torrentio"})
	}
	return out
}

func BenchmarkRank(b *testing.B) {
	year, minSeeders := 2024, 3
	movie := func() rankFilters {
		return rankFilters{ExcludeCam: true, ResultCap: 100, ExpectedYear: &year, MinSeeders: &minSeeders,
			Resolutions:         []string{"2160p", "1080p"},
			ExpectedTitleTokens: titleTokens("Dune Part Two")}
	}
	episode := func() rankFilters {
		return rankFilters{ExcludeCam: true, ResultCap: 100, PreferResolution: "1080p",
			Episode: &[2]int{1, 5}, ShowTokens: titleTokens("Pokemon")}
	}
	for _, n := range []int{100, 500, 2000} {
		streams := releases(b, n)
		b.Run(fmt.Sprintf("movie/%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = rankStreams(streams, movie())
			}
		})
		b.Run(fmt.Sprintf("episode/%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = rankStreams(streams, episode())
			}
		})
	}
}

func BenchmarkParseStremioStreams(b *testing.B) {
	for _, name := range []string{"streams-50.json", "streams-500.json"} {
		body := fixture(b, name)
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = parseStremioStreams(body, "torrentio")
			}
		})
	}
}

func BenchmarkDecodeListing(b *testing.B) {
	body := fixture(b, "torbox-listing.json")
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dec := json.NewDecoder(strings.NewReader(string(body)))
		if _, ok, _ := decodeListing(dec); !ok {
			b.Fatal("listing did not decode")
		}
	}
}

func benchKeyring() *sealKeyring {
	var priv [x25519KeySize]byte
	for i := range priv {
		priv[i] = byte(i + 1)
	}
	return &sealKeyring{keys: []sealKeypair{newSealKeypair(priv)}}
}

func BenchmarkDecodeConfig(b *testing.B) {
	plain := fixture(b, "config.json")
	kr := benchKeyring()
	sealed, err := seal(&kr.keys[0].pub, plain)
	if err != nil {
		b.Fatal(err)
	}
	plainSeg := b64urlEncode(plain)
	sealedSeg := b64urlEncode(append([]byte{sealedVersion}, sealed...))
	b.Run("plain", func(b *testing.B) {
		if _, ok := decodeConfig(nil, plainSeg); !ok {
			b.Fatal("plain config rejected")
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = decodeConfig(nil, plainSeg)
		}
	})
	b.Run("sealed", func(b *testing.B) {
		if _, ok := decodeConfig(kr, sealedSeg); !ok {
			b.Fatal("sealed config rejected")
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = decodeConfig(kr, sealedSeg)
		}
	})
}

func BenchmarkTicket(b *testing.B) {
	tk := newTicketKeys(benchKeyring())
	idx, season, episode := 3, 1, 5
	config := &Config{Debrid: []DebridAccount{{ServiceTorBox, strings.Repeat("t", 40)}, {ServiceRealDebrid, strings.Repeat("R", 52)}},
		IID: "AAECAwQFBgcICQoLDA0ODw", Epoch: 3}
	target := PlayTarget{InfoHash: strings.Repeat("ab", 20), FileIdx: &idx, Season: &season, Episode: &episode,
		ReleaseSize: 4 << 30, Title: "Show.S01E05.1080p.WEB-DL.DDP5.1.H.264-NTb", IMDb: "tt0944947"}
	exp := time.Now().Add(24 * time.Hour)
	ticket := tk.mint(config, target, exp)
	b.Run("mint", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = tk.mint(config, target, exp)
		}
	})
	b.Run("open", func(b *testing.B) {
		now := time.Now()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, err := tk.open(ticket, now); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkProbe(b *testing.B) {
	for _, f := range []struct{ name, kind string }{
		{"tracks.mkv", "head"}, {"hi10p.mkv", "head"}, {"sample.mp4", "mp4"}, {"hevc10.mp4", "mp4"},
		{"multi-audio.mp4", "mp4"}, {"moov-at-end.mp4", "mp4"}, {"xvid.avi", "avi"},
	} {
		head := testdata(b, f.name)
		b.Run(f.name, func(b *testing.B) {
			b.SetBytes(int64(len(head)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				switch f.kind {
				case "head":
					_, _ = ParseHead(head)
				case "mp4":
					_, _ = parseMP4(head)
				default:
					_, _ = parseAVI(head)
				}
			}
		})
	}
}

func BenchmarkMemoryCache(b *testing.B) {
	value := strings.Repeat("v", 1024)
	b.Run("put_evicting", func(b *testing.B) {
		c := NewMemoryCache(1 << 20)
		keys := make([]string, 1<<16)
		for i := range keys {
			keys[i] = fmt.Sprintf("list:v1:%040x", i)
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			c.Put(keys[i&(len(keys)-1)], value, time.Hour)
		}
	})
	b.Run("get_hit", func(b *testing.B) {
		c := NewMemoryCache(4 << 20)
		keys := make([]string, 512)
		for i := range keys {
			keys[i] = fmt.Sprintf("list:v1:%040x", i)
			c.Put(keys[i], value, time.Hour)
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, ok := c.Get(keys[i&511]); !ok {
				b.Fatal("miss")
			}
		}
	})
	b.Run("get_miss", func(b *testing.B) {
		c := NewMemoryCache(4 << 20)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = c.Get("absent")
		}
	})
}

func BenchmarkTieredCache(b *testing.B) {
	value := strings.Repeat("v", 4096)
	b.Run("put_durable", func(b *testing.B) {
		c := NewTieredCache(1<<20, b.TempDir())
		c.maxBytes = 0 // measure the write, not a budget sweep
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			c.Put(fmt.Sprintf("probe:v1:%040x", i&1023), value, time.Hour)
		}
	})
	b.Run("get_disk_hit", func(b *testing.B) {
		c := NewTieredCache(1, b.TempDir()) // memory tier too small to hold anything: every Get reads the file
		keys := make([]string, 256)
		for i := range keys {
			keys[i] = fmt.Sprintf("probe:v1:%040x", i)
			c.Put(keys[i], value, time.Hour)
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, ok := c.Get(keys[i&255]); !ok {
				b.Fatal("miss")
			}
		}
	})
}
