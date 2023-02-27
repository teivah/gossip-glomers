package main

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

const (
	maxRetry = 100
)

func init() {
	f, err := os.OpenFile("/tmp/maelstrom.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(f)
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	s := &server{
		n:     n,
		kv:    kv,
		store: make(map[int]int),
	}

	n.Handle("init", wrap(s.initHandler))
	n.Handle("txn", wrap(s.txnHandler))
	n.Handle("sync", wrap(s.syncHandler))

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func wrap(f func(msg maelstrom.Message) error) func(msg maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		if err := f(msg); err != nil {
			log.Error(err)
			return err
		}
		return nil
	}
}

type server struct {
	n      *maelstrom.Node
	kv     *maelstrom.KV
	nodeID string
	id     int

	mu    sync.Mutex
	store map[int]int
}

func (s *server) initHandler(_ maelstrom.Message) error {
	s.nodeID = s.n.ID()
	id, err := strconv.Atoi(s.nodeID[1:])
	if err != nil {
		return err
	}
	s.id = id
	return nil
}

type txnReq struct {
	MsgId int     `json:"msg_id"`
	Txn   [][]any `json:"txn"`
}

func (s *server) txnHandler(msg maelstrom.Message) error {
	var req txnReq
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	res := make([][]any, 0, len(req.Txn))
	changes := make(map[int]int)
	s.mu.Lock()
	for _, txn := range req.Txn {
		op := make([]any, len(txn))
		copy(op, txn)
		if txn[0] == "r" {
			a := int(txn[1].(float64))
			op[2] = s.store[a]
		} else if txn[0] == "w" {
			a := int(txn[1].(float64))
			b := int(txn[2].(float64))
			s.store[a] = b
			changes[a] = b
		}
		res = append(res, op)
	}

	s.mu.Unlock()

	go func() {
		body := syncReq{
			Type:   "sync",
			Values: changes,
		}
		for _, nodeID := range s.n.NodeIDs() {
			if nodeID == s.nodeID {
				continue
			}
			if err := s.rpcWithRetry(nodeID, body, maxRetry); err != nil {
				log.Error(err)
			}
		}
	}()

	return s.n.Reply(msg, map[string]any{
		"type": "txn_ok",
		"txn":  res,
	})
}

type syncReq struct {
	Type   string      `json:"type"`
	Values map[int]int `json:"values"`
}

func (s *server) syncHandler(msg maelstrom.Message) error {
	var req syncReq
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	s.mu.Lock()
	for k, v := range req.Values {
		s.store[k] = v
	}
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type": "sync_ok",
	})
}

func (s *server) rpcWithRetry(dst string, body any, retry int) error {
	var err error
	for i := 0; i < retry; i++ {
		if err = s.rpc(dst, body); err != nil {
			// Sleep and retry
			time.Sleep(100 * time.Duration(i) * time.Millisecond)
			continue
		}
		return nil
	}
	return err
}

func (s *server) rpc(dst string, body any) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := s.n.SyncRPC(ctx, dst, body)
	return err
}
