package main

import (
	"encoding/json"
	"os"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
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
		}
		res = append(res, op)
	}
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type": "txn_ok",
		"txn":  res,
	})
}
