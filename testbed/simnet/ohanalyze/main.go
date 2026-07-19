// Command ohanalyze bins per-node overhead (from -overhead-out JSON) by
// log-distance from topic 0 in the ID space, so we can see how many
// messages/bytes each node sends/receives as a function of its position
// relative to the topic.
package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// loadManifest reads registrants-t*.json (written by -snapshot-dir) and returns
// id -> logdist-to-its-own-topic, so multi-topic runs bin by each node's own
// topic rather than topic 0.
func loadManifest(dir string) map[string]int {
	files, _ := filepath.Glob(filepath.Join(dir, "registrants-t*.json"))
	m := make(map[string]int)
	for _, fp := range files {
		data, err := os.ReadFile(fp)
		if err != nil {
			continue
		}
		var recs []struct {
			ID      string `json:"id"`
			LogDist int    `json:"logdist"`
		}
		if json.Unmarshal(data, &recs) != nil {
			continue
		}
		for _, r := range recs {
			m[strings.ToLower(r.ID)] = r.LogDist
		}
	}
	return m
}

// makeTopic mirrors the testbed's topic-id derivation.
func makeTopic(i int) enode.ID {
	const phi = 0.6180339887498949
	frac := math.Mod(float64(i+1)*phi, 1.0)
	h := crypto.Keccak256([]byte{0x74, 0x6f, 0x70, byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)})
	var t enode.ID
	copy(t[:], h)
	binary.BigEndian.PutUint64(t[:8], uint64(frac*math.Ldexp(1, 63))<<1)
	return t
}

type rec struct {
	ID      string           `json:"id"`
	TxPkts  int64            `json:"txPkts"`
	TxBytes int64            `json:"txBytes"`
	RxPkts  int64            `json:"rxPkts"`
	RxBytes int64            `json:"rxBytes"`
	TQRcv   int64            `json:"tqRcv"`
	Traffic map[string]int64 `json:"traffic"`
}

type agg struct {
	n, txp, txb, rxp, rxb, tq int64
	traf                      map[string]int64 // per-message-type sums
}

// writeTrafficCSV dumps per-logdist-bin, per-node mean of each traffic key
// (plus total tx/rx bytes and nNodes) for plotting. Far (256) -> close.
func writeTrafficCSV(path string, bins map[int]*agg, tot *agg) {
	keys := make([]string, 0, len(tot.traf))
	for k := range tot.traf {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer f.Close()
	fmt.Fprint(f, "logdist,nNodes,txB,rxB")
	for _, k := range keys {
		fmt.Fprint(f, ","+k)
	}
	fmt.Fprintln(f)
	lds := make([]int, 0, len(bins))
	for d := range bins {
		lds = append(lds, d)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(lds)))
	for _, d := range lds {
		a := bins[d]
		n := float64(a.n)
		fmt.Fprintf(f, "%d,%d,%.1f,%.1f", d, a.n, float64(a.txb)/n, float64(a.rxb)/n)
		for _, k := range keys {
			fmt.Fprintf(f, ",%.1f", float64(a.traf[k])/n)
		}
		fmt.Fprintln(f)
	}
}

func main() {
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var recs []rec
	if err := json.Unmarshal(data, &recs); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	topic := makeTopic(0)

	// Args after overhead.json: a snaps dir (bin by each node's own-topic
	// logdist, multi-topic) and/or a *.csv path to dump the per-bin traffic
	// breakdown. Without a snaps dir, bin by logdist to topic 0.
	var manifest map[string]int
	var csvOut string
	for _, a := range os.Args[2:] {
		if strings.HasSuffix(a, ".csv") {
			csvOut = a
		} else {
			manifest = loadManifest(a)
			fmt.Printf("# manifest: %d nodes mapped to own-topic logdist\n", len(manifest))
		}
	}

	// Bin by logdist. logdist small => close to topic (long common prefix);
	// logdist 256 => differ in the first bit (far).
	bins := map[int]*agg{}
	var tot agg
	var unmapped int
	for _, r := range recs {
		var d int
		if manifest != nil {
			dd, ok := manifest[strings.ToLower(r.ID)]
			if !ok {
				unmapped++
				continue
			}
			d = dd
		} else {
			b, _ := hex.DecodeString(r.ID)
			var id enode.ID
			copy(id[:], b)
			d = enode.LogDist(topic, id)
		}
		a := bins[d]
		if a == nil {
			a = &agg{traf: map[string]int64{}}
			bins[d] = a
		}
		a.n++
		a.txp += r.TxPkts
		a.txb += r.TxBytes
		a.rxp += r.RxPkts
		a.rxb += r.RxBytes
		a.tq += r.TQRcv
		if tot.traf == nil {
			tot.traf = map[string]int64{}
		}
		for k, v := range r.Traffic {
			a.traf[k] += v
			tot.traf[k] += v
		}
		tot.n++
		tot.txp += r.TxPkts
		tot.txb += r.TxBytes
		tot.rxp += r.RxPkts
		tot.rxb += r.RxBytes
		tot.tq += r.TQRcv
	}

	if csvOut != "" {
		writeTrafficCSV(csvOut, bins, &tot)
	}

	keys := make([]int, 0, len(bins))
	for k := range bins {
		keys = append(keys, k)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(keys))) // far (256) -> close

	fmt.Printf("%-8s %7s %11s %10s %11s %10s %9s\n",
		"logdist", "nNodes", "meanTxPkts", "meanTxKB", "meanRxPkts", "meanRxKB", "meanTQrcv")
	pr := func(label string, a *agg) {
		if a.n == 0 {
			return
		}
		fmt.Printf("%-8s %7d %11.0f %10.1f %11.0f %10.1f %9.1f\n",
			label, a.n,
			float64(a.txp)/float64(a.n),
			float64(a.txb)/float64(a.n)/1024,
			float64(a.rxp)/float64(a.n),
			float64(a.rxb)/float64(a.n)/1024,
			float64(a.tq)/float64(a.n))
	}
	for _, k := range keys {
		pr(fmt.Sprintf("%d", k), bins[k])
	}
	fmt.Println("----")
	pr("ALL", &tot)
	if unmapped > 0 {
		fmt.Printf("# %d nodes not in manifest (skipped)\n", unmapped)
	}
}
