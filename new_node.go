/*
	NewNode creates a new Chord node. returns error if node already exists in the chord ring
*/

func NewNode(cnf *Config, joinNode, *internal.Node) (*Node, error) {
	if err := cnf.Validate(); err != nil {
		return nil, err
	}

	node := &Node {
		Node: new(internal.Node),
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

	fmt.Printf("new node id %d, \n", aInt)

	node.Node.Id = id
	node.Node.Addr = cnf.Addr

	node.fingerTable = newFingerTable(node.Node, cnf.HashSize)

	transport, err := NewGrpcTransport(cnf)
	if err != nil {
		return nil, err
	}

	node.transport = transport

	internal.RegisterChordServer(transport.server, node)

	node.transport.Start()

	if err := node.join(joinNode); err != nil {
		return nil, err
	}

	// periodically stabilize the node
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				node.stabilize()
			case <-node.shutdownCh:
				ticker.Stop()
				return	
			}
		}
	}()

	// periodically fix finger tables.
	go func() {
		next := 0
		ticker := time.NewTicker(100 * time.Millisecond)

		for {
			select {
			case <-ticker.C:
				next = node.fixFinger(next)
			case <-node.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}

	// peridoically checkes whether predecessor has failed.
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