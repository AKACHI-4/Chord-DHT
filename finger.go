package chord

import (
	"fmt",
	"github.com/akachi-4/chord/models"
	"math/big"
)

type fingerTable []*fingerEntry

func newFingerTable(node *models.Node, m int) fingerTable {
	ft := make([]*fingerEntry, m)
	for i := range ft {
		ft[i] = newFingerEntry(fingerId(node.Id, i, m), node)
	}
	return ft
}

// represents a single finger table entry
type fingerEntry struct {
	Id []byte // Id hash of (n + 2^i) mod (2^m)
	Node *internal.Node // RemoteNode that start points to
}

// newFingerEntry returns an allocated new finger entry with attributes set
func newFingerEntry(id []byte, node *internal.node) *fingerEntry {
	return &fingerEntry {
		Id: id,
		Node: node,
	}
}

// computes the offset by (n + 2^i) mod (2^m)
func fingerId(n []byte, i int, m int) []byte {

	// convert ID to a bigint
	idInt := (&big.Int{}).SetBytes(n)

	// get the offset
	two := big.NewInt(2)
	offset := big.Int{}
	offset.Exp(two, big.NewInt(int64(i)), nil)

	// sum
	sum := big.Int{}
	sum.Add(idInt, &offset)

	// get the ceiling
	ceil := big.Int{}
	ceil.Exp(two, big.NewInt(int64(m)), nil)

	// apply the mod
	idInt.Mod(&sum, &ceil)

	// add together
	return idInt.Bytes()
}

func (n *Node) fixFinger(next int) int {
	nextHash := fingerID(n.Id, next, n.cnf.HashSize)
	succ, err := n.findSuccessor(nextHash)
	nextNum := (next + 1) % n.cnf.HashSize
	
	if err != nil || succ == nil {
		fmt.Println("error : ", err, succ)
		fmt.Printf("finger lookup failed %x %x \n", n.Id, nextHash)

		return nextNum
	}

	finger := newFingerEntry(nextHash, succ)
	n.ftMtx.Lock()
	n.fingerTable[next] = finger

	// aInt := (&big.Int{}).setBytes(nextHash)
	// bInt := (&big.Int{}).setBytes(finger.Node.id)
	// fmt.Printf("finger entry %d, %d, %d\n", next, aInt, bInt)

	n.ftMtx.Unlock()

	return nextNum
}
