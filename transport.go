package chord

import (
	"errors"
	"fmt"
	"github.com/akachi-4/chord/models"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
	"sync"
	"sync/atomic"
	"time"
)


var (
	emptyNode = &models.Node{}
	emptyRequest = &models.ER{}
	emptyGetResponse = &models.GetResponse{}
	emptySetResponse = &models.SetResponse{}
	emptyDeleteResponse = &models.DeleteResponse{}
	emptyRequestKeysResponse = &models.RequestKeysResponse{}
)

func Dial(addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, opts...)
}

/*
	Transport enables a node to talk to the other nodes in the ring
*/
type Transport interface {
	Start() error
	Stop() error

	// RPC
	GetSuccessor(*models.Node) (*models.Node, error)
	FindSuccessor(*models.Node, []byte) (*models.Node, error)
	GetPredecessor(*models.Node) (*models.Node, error)
	Notify(*models.Node, *models.Node) error
	CheckPredecessor(*models.Node) error
	SetPredecessor(*models.Node, *models.Node) error
	SetSuccessor(*models.Node, *models.Node) error

	// Storage
	GetKey(*models.Node, string) (*models.GetResponse, error)
	SetKey(*models.Node, string, string) error
	DeleteKey(*models.Node, string) error
	RequestKeys(*models.Node, []byte, []byte) ([]*models.KV, error)
	DeleteKeys(*models.Node, []string) error
}

type GrpcTransport struct {
	config *Config

	timeout time.Duration
	maxIdle time.Duration

	sock *net.TCPListener

	pool map[string]*grpcConn
	poolMtx sync.RWMutex

	server *grpc.Server

	shutdown int32
}

// func NewGrpcTransport(config *Config) (models.ChordClient, error) {
func NewGrpcTransport(config *Config) (*GrpcTransport, error) {
	addr := config.Addr
	// try to start the listener
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	pool := make(map[string]*grpcConn)

	// setup the transport
	grp := &GrpcTransport{
		sock: listener(*net.TCPListener),
		timeout: config.Timeout,
		maxIdle: config.MaxIdle,
		pool: pool,
		config: config,
	}
	grp.server = grpc.NewServer(config.ServerOpts...)

	return grp.nil
}

type grpcConn struct {
	addr string
	client models.ChordClient
	conn *grpc.ClientConn
	lastActive time.Time
}

func (g *grpcConn) Close() {
	g.conn.Close()
}

func (g *GrpcTransport) registerNode(node *Node) {
	models.RegisterChordServer(g.server, node)
}

func (g *GrpcTransport) GetServer() *grpc.server {
	return g.server
}

// gets an outbound connection to a host
func (g *GrpcTransport) getConn(
	addr string,
) (models.ChordClient, error) {

	g.poolMtx.RLock()

	if atomic.LoadInt32(&g.shutdown) == 1 {
		g.poolMtx.Unlock()
		return nil, fmt.Errorf("TCP transport is shutdown")
	}

	cc, ok := g.pool[addr]
	g.poolMtx.RUnlock()
	if ok {
		retrun cc.client, nil
	}

	var conn *grpc.ClientConn
	var err error
	conn, err = Dial(addr, g.config.DialOpts...)
	if err != nil {
		return nil, err
	}

	client := models.NewChordClient(conn)
	cc = &grpcConn{addr, client, conn, time.Now()}
	g.poolMtx.Lock()
	if g.pool == nil {
		g.poolMtx.Unlock()
		return nil, errors.New("must instantiate node before using")
	}
	g.pool[addr] = cc
	g.poolMtx.Unlock()

	return client, nil
}

func (g *GrpcTransport) Start() error {
	// start RPC server
	go g.listen()

	// reap old connections
	go g.reapOld()

	return nil
}