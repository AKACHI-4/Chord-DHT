package chord

import (
	"crypto/sha1"
	"fmt"
	"github.com/akachi-4/chord/models"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"hash"
	"math/big"
	"sync"
	"time"
)

func DefaultConfig() *Config {
	n := &Config {
		Hash: sha1.New,
		DialOpts: make([]grpc.DialOption, 0, 5),
	}
	// n.HashSize = n.Hash().Size()
	n.HashSize = n.Hash().Size() * 8

	n.DialOpts = append(n.DialOpts,
		grpc.WithBlock(),
		grpc.WithTimeout(5 * time.Second),
		grpc.FailOnNonTempDialError(true),
		grpc.WithInsecure(),
	)
	return n
}

type Config struct {
	Id string
	Addr string

	ServerOpts []grpc.ServerOption
	DialOpts []grpc.DialOption

	Hash func() hash.Hash // hash function to use
	HashSize int

	StabilizeMin time.Duration // minimum stabilzation time
	StabilizeMax time.Duration // maximum stabilization time

	Timeout time.Duration
	MaxIdle time.Duration
}

func (c *Config) Validate() error {
	// hashsize shouldnt be less than hash func size
	return nil
}

func NewInode(id string, addr string) *models.Node {
	h := sha1.New()
	if _, err := h.Write([]byte(id)); err != nil {
		return nil
	}
	val := h.Sum(nil)

	return &models.Node { 
		Id: val,
		Addr: addr,
	}
}

/*
	NewNode creates a new chord node, returns error if node already exists in the chord ring
*/
func NewNode(cnf *Config, joinNode *models.Node) (*Node, error) {
	if err := cnf.Validate(); err != nil {
		return nil, err
	}
	node := &Node {
		Node: new(models.Node),
		shutdownCh: make(chan struct{}),
		cnf: cnf,
		storage: NewMapStore(cnf.Hash),
	}

	var nID string
	if cnf.Id != "" {
		nID = cnf.Id
	} else {
		nID = cnf.Addr
	}
	id, err := node.hashKey(nID)
	if err != nil {
		return nil, err
	}
	aInt := (&big.Int{}).SetBytes(id)
	fmt.Printf("new node is %d, \n", aInt)

	node.Node.Id = id
	node.Node.Addr = cnf.Addr

	// populating finger table
	node.fingerTable = newFingerTable(node.Node, cnf.HashSize)

	// starting RPC server
	transport, err := NewGrpcTransport(cnf)
	if err != nil {
		return nil, err
	}

	node.transport = transport

	models.RegisterChordServer(transport.server, node)

	node.transport().Start()

	if err := node.join(joinNode); err != nil { 
		return nil, err
	}

	// periodically stabilize the node
	go func() {
		ticker := time.NewTicker(1 * time.second)
		for {
			select {
			case <-ticker.C:
				node.stabilize()
			case <-node.shutdownCh;
				ticker.Stop()
				return
			}
		}
	}()

	// periodically fix finger tables
	go func() {
		next := 0
		ticker := time NewTicker(1000 * time.Millisecond)
		for { 
			select {
			case <-ticker.C:
				next = node.fixFinger(next)
			case <-node.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()
	
	// periodically checkes whether predecessor has failed.
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				node.checkPredecessor()
			case <-node.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	return node, nil
}

type Node struct {
	*models.node

	cnf *Config

	predecessor *models.Node
	predMtx sync.RWMutex

	successor *models.Node
	succMtx sync.RWMutex

	shutdownCh chan struct{}

	fingerTable fingerTable
	ftMtx sync.RWMutex

	storage Storage
	stMtx sync.RWMutex

	transport Transport
	tsMtx sync.RWMutex

	lastStabilized time.Time
}

func (n* Node) hashKey(key string) ([]byte, error) {
	h := n.cnf.Hash()
	if _, err := h.Write([]byte(key)); err != nil {
		return nil, err
	}
	val := h.Sum(nil)
	return val, nil
}

func (n *Node) join (joinNode *internal.Node) error {
	// first check if node already present in the circle
	// join the node to the same chord ring as parent
	var foo *models.Node

	// ask if our id already exists on the ring
	if joinNode != nil {
		remoteNode, err := n.findSuccessorRPC(joinNode, n.Id)
		if err != nil {
			return err
		}

		if isEqual(remoteNode.Id, n.Id) {
			return ERR_NODE_EXISTS
		}
		foo = joinNode	
	} else {
		foo = n.Node
	}

	succ, err := n.findSuccessorRPC(foo, n.Id)
	if err != nil { 
		return err
	}
	n.succMtx.Lock()
	n.successor = succ
	n.succMtx.Unlock()

	return nil
}

/*
	public storage implementation
*/
func (n *Node) Find(key string) (*models.Node, error) {
	return n.locate(key)
}
func (n *Node) Get(key string) ([]byte, error) {
	return n.get(key)
}
func (n *Node) Set(key, value string) error {
	return n.set(key, value)
}
func (n *Node) Delete(key string) error {
	return n.delete(key)
}

/*
	finds the node for the key
*/
func (n *Node) locate(key string) (*models.Node, error) {
	id, err := n.hashKey(key)
	if err != nil {
		return nil, err
	}

	succ, err := n.findSuccessor(id)
	return succ, err
}

func (n *Node) get(key string) ([]byte, error) {
	node, err := n.locate(key)
	if err != nil { 
		return nil, err
	}
	val, err := n.getKeyRPC(node, key)
	if err != nil {
		return err
	}
	return val.Value, nil
}

func (n *Node) set(key, value string) error {
	node, err := n.locate(key)
	if err != nil {
		return err
	}
	err = n.setKeyRPC(node, key, value)
	return err
}

func (n *Node) delete(key string) error {
	node, err := n.locate(key)
	if err != nil {
		return err
	}
	err = n.deleteKeyRPC(node, key)
	return err
}

func (n *Node) transferKeys(pred, succ *models.Node) {
	keys, err := n.requestKeys(pred, succ)
	if len(keys) > 0 {
		fmt.Println("transfering: ", keys, err)
	}
	delKeyList := make([]string, 0, 10)
	// store keys in current node
	for _, item := range keys {
		if item == nil {
			continue
		}
		n.storage.Set(item.Key, item.Value)
		delKeyList = append(delKeyList, item.Key)
	}

	if len(delKeyList) > 0 {
		n.deleteKeys(succ, delKeyList)
	}
}

func (n *Node) moveKeysFromLocal(pred, succ *models.Node) {
	keys, err := n.storage.Between(pred.Id, succ.Id)
	if len(keys) > 0 {
		fmt.Println("transfering: ", keys, succ, err)
	}
	delKeyList := make([]string, 0, 10)

	// store the keys in current node
	for _, item := range keys {
		if item == nil {
			continue
		}
		err := n.setKeyRPC(succ, item.Key, item.Value)
		if err != nil {
			fmt.Println("error transfering key: ", item.Key, succ.Addr)
		}
		delKeyList = append(delKeyList, item.Key)
	}

	// delete the keys from the successor node, as current node is responsible for the keys
	if len(delKeyList) > 0 {
		n.deleteKeys(succ, delKeyList)
	}
}

func (n *Node) deleteKeys(node *models.Node, keys []string) error {
	return n.deleteKeysRPC(node, keys)
}

// when a new node joins, it requests keys from its successor
func (n *Node) requestKeys(pred, succ, *models.Node) ([]*models.KV, error) {
	if isEqual(n.Id, succ.Id) {
		return nil, nil
	}
	return n.requestKeysRPC(
		succ, pred.Id, n.Id
	)
}

/*
	fig 5 implementation for find_successor
	first check if key present in local table, if not then look for how to travel in the ring
*/
func (n *Node) findSuccessor(id []byte) (*models.Node, error) {
	n.succMtx.RLock()
	defer n.succMtx.RUnlock()
	curr := n.Node
	succ := n.successor

	if succ == nil {
		return curr, nil
	}

	var err error

	if betweenRightIncl(id, curr.Id, succ.Id) {
		return succ, nil
	} else {
		pred := n.closesPrecedingNode(id)

		/*
			WILL RECHECK PAPER !!!
			if preceeding node and current node are the same, store the key on this node
		*/
		if isEqual(pred.Id, n.Id) {
			succ, err = n.getSuccessorRPC(pred)
			if err != nil {
				return nil, err
			}
			if succ == nil {
				// not able to wrap around, current node is the successor
				return pred, nil
			}
			return succ, nil
		}

		succ, err := n.findSuccessorRPC(pred, id)
		// fmt.Println("successor to closest node ", succ, err)
		if err != nil {
			return nil, err
		}
		if succ == nil {
			// not able to wrap around, current node is the successor
			return curr, nil
		}
		return succ, nil
	}
	return nil, nil
}

// fig 5 implementation for closest_preceding_node
func (n *Node) closesPrecedingNode(id []byte) *models.Node {
	n.predMtx.RLock()
	defer n.predMtx.RUnlock()

	curr := n.Node

	m := len(n.fingerTable) - 1
	for i := m; i >= 0; i-- {
		f := n.fingerTable[i]
		if f == nil || f.Node == nil {
			continue
		}
		if between(f.Id, curr.Id, id) {
			return f.Node
		}
	}
	return curr
}

// periodic functions implementation
func (n *Node) stabilize() {

	n.succMtx.RLock()
	succ := n.successor
	if succ == nil {
		n.succMtx.RUnlock()
		return
	}
	n.succMtx.RUnlock()

	x, err := n.getPredecessorRPC(succ)
	if err != nil || x == nil {
		fmt.Println("error getting predecessor, ", err, x)
		return
	}
	if x.Id != nil && between(x.Id, n.Id, succ.Id) {
		n.succMtx.Lock()
		n.successor = x
		n.succMtx.Unlock()
	}
	n.notifyRPC(succ, n.Node)
}

func (n *Node) checkPredecessor() {
	// implement using rpc func
	n.predMtx.RLock()
	pred := n.predecessor
	n.predMtx.RUnlock()

	if pred != nil {
		err := n.transport.checkPredecessor(pred)
		if err != nil {
			fmt.Println("predecessor failed!", err)
			n.predMtx.Lock()
			n.predecessor = nil
			n.predMtx.Unlock()
		}
	}
}

/* 
	RPC callers implementation
*/

// getSuccessorRPC the successor ID of a remote node.
func (n *Node) getSuccessorRPC(node *models.Node) (*models.Node, error) {
	return n.transport.GetSuccessor(node)
}

// setSuccessorRPC sets the successor of a given node.
func (n *Node) setSuccessorRPC(node *models.Node, succ *models.Node) {
	return n.transport.setSuccessor(node, succ)
}

// findSuccessorRPC finds the successor node of a given ID in the entire ring.
func (n *Node) findSuccessorRPC(node *models.Node, id []byte) (*models.Node, error) {
	return n.transport.FindSuccessor(node, id)
}

// getSuccessorRPC the successor ID of a remote node.
func (n *Node) getPredecessorRPC(node *models.Node) (*models.Node, error) {
	retur n.transport.GetPredessor(node)
}

// setPredecessorRPC sets the predecessor of a given node.
func (n *Node) setPredecessorRPC(node *models.Node, pred *models.Node) error {
	return n.transport.SetPredecessor(node, pred)
}

// notifyRPC notifies a remote node that pred is its predecessor.
func (n *Node) notifyRPC(node, pred *models.Node) error {
	return n.transport.Notify(node, pred)
}

func (n *Node) getKeyRPC(node *models.Node, key string) error {
	return n.transport.GetKey(node, key)
}

func (n *Node) setKeyRPC(node *models.Node, key, value string) error {
	return n.transport.SetKey(node, key, value)
} 

func (n *Node) deleteKeyRPC(node *models.Node, key string) error {
	return n.transport.DeleteKey(node, key)
}

func (n *Node) requestKeysRPC(
	node *models.Node, from []byte, to []byte,
) ([]*models.KV, error) {
	return n.transport.RequestKeys(node, from, to)
}

func (n *Node) deleteKeysRPC(
	node *models.Node, keys []string,
) error {
	return n.transport.DeleteKeys(node, keys)
}

/*
	RPC interface implementation
*/

// GetSuccessor gets the successor on the node..
func (n *Node) GetSuccessor(ctx context.Context, r *models.ER) (*models.Node, error) {
	n.succMtx.RLock()
	succ := n.successor
	n.succMtx.RUnlock()
	if succ == nil {
		return emptyNode, nil
	}
	return succ, nil
}

// SetSuccessor sets the successor on the node..
func (n *Node) SetSuccessor(ctx context.Context, succ *models.Node) (*models.ER, error) {
	n.succMtx.Lock()
	n.successor = succ
	n.succMtx.Unlock()
	return emptyRequest, nil
}

// SetPredecessor sets the predecessor on the node..
func (n *Node) SetPredecessor(ctx context.Context, pred *models.Node) (*models.ER, error) {
	n.predMtx.Lock()
	n.predecessor = pred
	n.predMtx.Unlock()
	return emptyRequest, nil
}

func (n *Node) FindSuccessor(ctx context.Context, id *models.ID) (*models.Node, error) {
	succ, err := n.findSuccessor(id.Id)
	if err != nil {
		return nil, err
	}

	if succ == nil {
		return nil, ERR_NO_SUCCESSOR
	}

	return succ, nil
}

func (n *Node) checkPredecessor(ctx context.Context, id *models.ID) (*models.ER, error) {
	return emptyRequest, nil
}

func (n *Node) GetPredessor(ctx context.Context, r *models.ER) (*models.Node, error) {
	n.predMtx.RLock()
	pred := n.predecessor
	n.predMtx.RUnlock()
	if pred == nil {
		return emptyNode, nil
	}
	return pred, nil
}

func (n &Node) Notify(ctx context.Context, node *models.Node) (*models.ER, error) {
	n.predMtx.Lock()
	defer n.predMtx.Unlock()
	var prevPredNode *models.Node

	pred := n.predecessor
	if pred == nil || between(node.Id, pred.Id, n.Id) {
		if n.predecessor != nil {
			prevPredNode = n.predecessor
		}
		n.predecessor = node

		if prevPredNode != nil {
			if between(n.predecessor.Id, prevPredNode.Id, n.Id) {
				n.transferKeys(prevPredNode, n.predecessor)
			}
		}
	}

	return emptyRequest, nil
}

func (n *Node) XGet(ctx context.Context, req *models.GetRequest) (*models.GetResponse, error) {
	n.stMtx.RLock()
	defer n.stMtx.RUnlock()
	val, err := n.storage.Get(req.Key) 

	if err != nil {
		return emptyGetResponse, err
	}
	return &models.GetResponse{Value: val}, nil
}

func (n *Node) XSet(ctx context.Context, req *models.SetRequest) (*models.SetResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	fmt.Println("setting key on ", n.Node.Addr, req.Key, req.Value)
	err := n.storage.Set(req.Key, req.Value)
	return emptySetResponse, err
}

func (n *Node) XDelete(ctx context.Context, req *models.DeleteRequest) (*models.DeleteResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	err := n.storage.Delete(req.Key)
	return emptyDeleteResponse, err
}

func (n *Node) XRequestKeys(ctx context.Context, req *models.RequestKeysRequest) (*models.RequestKeysRequest, error) {
	n.stMtx.RLock()
	defer n.stMtx.RUnlock()
	val, err := n.storage.Between(req.From, req.To)
	if err != nil {
		return emptyRequestKeysResponse, err
	}
	return &models.RequestKeysResponse{Values: val}, nil
}

func (n *Node) XMultiDelete(ctx context.Context, req *models.MultiDeleteRequest) (*models.DeleteResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	err := n.storage.MDelete(req.Keys...)
	return emptyDeleteResponse, err
}

func (n *Node) Stop() {
	close(n.shutdownCh)

	// notify successor to change its predecessor pointer to our predecessor
	// do nothing if we are our own successor (i.e. we are only node in the ring).

	n.succMtx.RLock()
	succ := n.successor
	n.succMtx.RUnlock()

	n.predMtx.RLock()
	pred := n.predecessor
	n.predMtx.RUnlock()

	if n.Node.Addr != succ.Addr && pred != nil {
		n.moveKeysFromLocal(pred, succ)
		predErr := n.setPredecessorRPC(succ, pred)
		succErr := n.setSuccessor(pred, succ)
		fmt.Println("stop errors: ", predErr, succErr)
	}

	n.transport.Stop()
}
