package enode

import (
	"net"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enr"
)

// TestMapKVNodeDB exercises the node DB operations on the map store.
func TestMapKVNodeDB(t *testing.T) {
	db := OpenMemoryDB()
	defer db.Close()
	var nodes []*Node
	for i := 0; i < 40; i++ {
		var r enr.Record
		r.Set(enr.IP(net.IPv4(10, 0, byte(i>>8), byte(i))))
		r.Set(enr.UDP(30303))
		n := SignNull(&r, ID{byte(i)})
		nodes = append(nodes, n)
		if err := db.UpdateNode(n); err != nil {
			t.Fatal(err)
		}
		db.UpdateLastPongReceived(n.ID(), n.IPAddr(), time.Now())
		db.UpdateFindFails(n.ID(), n.IPAddr(), i)
	}
	if got := db.Node(nodes[3].ID()); got == nil || got.ID() != nodes[3].ID() {
		t.Fatal("node not stored")
	}
	if db.FindFails(nodes[7].ID(), nodes[7].IPAddr()) != 7 {
		t.Fatal("find fails not stored")
	}
	db.UpdateFindFails(nodes[7].ID(), nodes[7].IPAddr(), 9)
	if db.FindFails(nodes[7].ID(), nodes[7].IPAddr()) != 9 {
		t.Fatal("overwrite lost")
	}
	seeds := db.QuerySeeds(10, time.Hour)
	if len(seeds) == 0 {
		t.Fatal("no seeds")
	}
	db.DeleteNode(nodes[3].ID())
	if db.Node(nodes[3].ID()) != nil {
		t.Fatal("node not deleted")
	}
}
