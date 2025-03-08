type Node struct {
	*internal.Node
	predecessor *internal.Node
	successor *internal.Node
	fingerTable fingerTable
	storage Storage
	transport Transport
}

func (n *Node) join (joinNode *internal.Node) error {
	// first check if node already present in the circle
	// join the node to the same chord ring as parent
	var foo *internal.Node

	// ask if our id already exists on the ring
	if joinNode != nil {
		remoteNode, err := n.findSuccessorRPC(joinNode, n.Id)
		if err != nil {
			return err
		}
		if isEqual(remoteNOde.Id, n.Id) {
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