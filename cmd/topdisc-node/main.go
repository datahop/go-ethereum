// topdisc-node is a full devp2p node whose only job is to register a topic and
// keep its peer slots filled from topic search: geth's p2p server, real RLPx
// connections, and the dialer fed by the TopicSearch iterator.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// lazyIter hands the dialer a TopicSearch iterator once the server is up.
// DialCandidates must exist before Start, but the discv5 instance only after.
type lazyIter struct {
	ready <-chan struct{}
	open  func() enode.Iterator
	once  sync.Once
	it    enode.Iterator
}

func (l *lazyIter) Next() bool {
	l.once.Do(func() { <-l.ready; l.it = l.open() })
	return l.it.Next()
}
func (l *lazyIter) Node() *enode.Node { return l.it.Node() }
func (l *lazyIter) Close() {
	if l.it != nil {
		l.it.Close()
	}
}

func main() {
	port := flag.Int("port", 30303, "TCP+UDP listen port")
	statusPort := flag.Int("status", 0, "status HTTP port (0 = port+10000)")
	boot := flag.String("bootnodes", "", "comma-separated enode URLs")
	topicName := flag.String("topic", "topdisc-test", "topic to register and search")
	maxPeers := flag.Int("maxpeers", 50, "peer slots")
	dialRatio := flag.Int("dialratio", 3, "1/N of slots are dialed")
	ip := flag.String("ip", "127.0.0.1", "address to listen on and advertise")
	verbosity := flag.Int("v", 2, "log verbosity")
	flag.Parse()
	if *statusPort == 0 {
		*statusPort = *port + 10000
	}
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.FromLegacyLevel(*verbosity), false)))

	key, err := crypto.GenerateKey()
	if err != nil {
		panic(err)
	}
	topic := topicindex.TopicID(crypto.Keccak256Hash([]byte(*topicName)))
	var bootnodes []*enode.Node
	for _, u := range strings.Split(*boot, ",") {
		if u = strings.TrimSpace(u); u != "" {
			n, err := enode.Parse(enode.ValidSchemes, u)
			if err != nil {
				fmt.Fprintln(os.Stderr, "bad bootnode:", err)
				os.Exit(2)
			}
			bootnodes = append(bootnodes, n)
		}
	}

	ready := make(chan struct{})
	var srv *p2p.Server
	proto := p2p.Protocol{
		Name: "topdisc-test", Version: 1, Length: 1,
		// A connection with no shared capability is dropped as useless, so
		// hold one open that carries no messages.
		Run: func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
			for {
				if _, err := rw.ReadMsg(); err != nil {
					return err
				}
			}
		},
		DialCandidates: &lazyIter{ready: ready, open: func() enode.Iterator {
			return srv.DiscoveryV5().TopicSearch(topic, uint64(*port))
		}},
	}
	srv = &p2p.Server{Config: p2p.Config{
		PrivateKey: key, Name: "topdisc-node", MaxPeers: *maxPeers, DialRatio: *dialRatio,
		ListenAddr: fmt.Sprintf("%s:%d", *ip, *port), DiscoveryV4: false, DiscoveryV5: true,
		BootstrapNodesV5: bootnodes, Protocols: []p2p.Protocol{proto}, Logger: log.Root(),
	}}
	if err := srv.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "start:", err)
		os.Exit(1)
	}
	srv.LocalNode().SetFallbackIP(net.ParseIP(*ip))
	srv.DiscoveryV5().RegisterTopic(topic, uint64(*port))
	close(ready)
	log.Info("node up", "enode", srv.Self().URLv4(), "status", *statusPort)

	status := func() map[string]any {
		out, in := 0, 0
		for _, p := range srv.Peers() {
			if p.Inbound() {
				in++
			} else {
				out++
			}
		}
		d := srv.DiscoveryV5()
		return map[string]any{
			"enode": srv.Self().URLv4(), "peers": out + in, "outbound": out, "inbound": in,
			"table": len(d.AllNodes()), "ads_held": len(d.LocalTopicNodes(topic)),
		}
	}
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(status()) })
	go http.ListenAndServe(fmt.Sprintf("%s:%d", *ip, *statusPort), nil)
	go func() {
		for range time.Tick(30 * time.Second) {
			s := status()
			log.Info("status", "peers", s["peers"], "out", s["outbound"], "in", s["inbound"], "table", s["table"], "ads", s["ads_held"])
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	srv.Stop()
}
