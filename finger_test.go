package chord

import {
	"crypto/sha1"
	"fmt"
	"github.com/akachi-4/chord/models"
	"math/big"
	"reflect"
	"testing"
}

func TestNewFingerTable(t *testing.T) {
	g := newFingerTable(NewInode("8", "0.0.0.0:8003"), sha1.New().size())
	for i, j := range g {
		fmt.Printf("%d, %x, %x\n", i, j.Id, j.Node.Id)
	}
}

func TestNewFingerEntry(t *testing.T) {
	hashsize := sha1.New().Size() * 8
	id := GetHashID("0.0.0.0:8003")
	xInt := (&big.Int{}).SetBytes(id)
	for i := 0; i < 100; i++ {
		nextHash := fingerID(id, i, hashSize)
		aInt := (&big.Int{}).SetBytes(nextHash)
		fmt.Printf("%d, %d %d\n", xInt, aInt, hashSize)
	}
}

func Test_newFingerEntry(t *Testing.T) { 
	type args struct {
		id []byte
		node *models.Node
	}

	tests := []struct {
		name string
		args args
		want *fingerEntry
	}{
		// adding more test cases
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newFingerEntry(tt.args.id, tt.args.node); !reflect.DeepEqual(got, tt.want) {
				t.Error("newFingerEntry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_fingerID(t *testing.T) {
	type args struct {
		n []byte
		i int
		m int
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		// adding more test cases
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fingerID(tt.args.n, tt.args.i, tt.args.m); !reflect.DeepEqual(got, tt.want) {
				t.Error("fingerID() = %v, want %v", got, tt.want)
			}
		})
	}
}