package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

const (
	prefixCommit = "commit_"
	prefixLatest = "latest_"
	prefixEntry  = "entry_"
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
		n:  n,
		kv: kv,
	}

	n.Handle("init", wrap(s.initHandler))
	n.Handle("send", wrap(s.sendHandler))
	n.Handle("forward", wrap(s.sendHandler))
	n.Handle("poll", wrap(s.pollHandler))
	n.Handle("commit_offsets", wrap(s.commitHandler))
	n.Handle("list_committed_offsets", wrap(s.listHandler))

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
	mu     sync.Mutex
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

func (s *server) sendHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	key := body["key"].(string)

	ikey, err := strconv.Atoi(key)
	if err != nil {
		return err
	}
	if ikey%len(s.n.NodeIDs()) != s.id {
		body["type"] = "forward"
		res, err := s.n.SyncRPC(context.Background(), fmt.Sprintf("n%d", ikey%len(s.n.NodeIDs())), body)
		if err != nil {
			return err
		}

		var resBody map[string]any
		if err := json.Unmarshal(res.Body, &resBody); err != nil {
			return err
		}
		return s.n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": resBody["offset"],
		})
	}

	message := int(body["msg"].(float64))

	s.mu.Lock()
	defer s.mu.Unlock()
	keyLatest := fmt.Sprintf("%s%s", prefixLatest, key)
	offset, err := s.kv.ReadInt(context.Background(), keyLatest)
	if err != nil {
		rpcErr := err.(*maelstrom.RPCError)
		if rpcErr.Code == maelstrom.KeyDoesNotExist {
			offset = 0
		} else {
			return err
		}
	} else {
		offset++
	}

	if err := s.kv.Write(context.Background(), keyLatest, offset); err != nil {
		return err
	}

	keyEntry := fmt.Sprintf("%s%s", prefixEntry, key)
	v, err := s.kv.Read(context.Background(), keyEntry)
	if err != nil {
		rpcErr := err.(*maelstrom.RPCError)
		if rpcErr.Code == maelstrom.KeyDoesNotExist {
			v = ""
		} else {
			return err
		}
	}

	logs, err := toLogEntries(v.(string))
	if err != nil {
		return err
	}

	logs.entries = append(logs.entries, entry{
		offset:  offset,
		message: message,
	})

	err = s.kv.Write(context.Background(), keyEntry, logs.String())
	if err != nil {
		return err
	}

	return s.n.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

type entry struct {
	offset  int
	message int
}

func (e entry) String() string {
	return fmt.Sprintf("%d:%d", e.offset, e.message)
}

type logEntries struct {
	entries []entry
}

func toLogEntries(s string) (logEntries, error) {
	if s == "" {
		return logEntries{}, nil
	}

	split := strings.Split(s, ",")
	entries := make([]entry, 0, len(split))
	for _, v := range split {
		idx := strings.Index(v, ":")
		offset, err := strconv.Atoi(v[:idx])
		if err != nil {
			return logEntries{}, nil
		}
		message, err := strconv.Atoi(v[idx+1:])
		if err != nil {
			return logEntries{}, nil
		}
		entries = append(entries, entry{
			offset:  offset,
			message: message,
		})
	}
	return logEntries{
		entries: entries,
	}, nil
}

func (l logEntries) String() string {
	e := make([]string, len(l.entries))
	for i := 0; i < len(l.entries); i++ {
		e[i] = l.entries[i].String()
	}
	return strings.Join(e, ",")
}

type offsetReq struct {
	Offsets map[string]int `json:"offsets"`
}

func (s *server) pollHandler(msg maelstrom.Message) error {
	var req offsetReq
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	res := make(map[string][][2]int)
	for key, startingOffset := range req.Offsets {
		logs, err := s.getValue(key)
		if err != nil {
			return err
		}

		i := findOffset(logs, startingOffset)
		for ; i < len(logs.entries); i++ {
			e := logs.entries[i]
			res[key] = append(res[key], [2]int{e.offset, e.message})
		}
	}

	return s.n.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": res,
	})
}

func findOffset(logs logEntries, startingOffset int) int {
	l := 0
	r := len(logs.entries) - 1
	for l <= r {
		mid := l + (r-l)/2
		n := logs.entries[mid].offset
		if n == startingOffset {
			return mid
		}
		if n < startingOffset {
			l = mid + 1
		} else {
			r = mid - 1
		}
	}
	if l == 0 {
		return 0
	}
	return l
}

func (s *server) getValue(key string) (logEntries, error) {
	v, err := s.kv.Read(context.Background(), fmt.Sprintf("%s%s", prefixEntry, key))
	if err != nil {
		return logEntries{}, err
	}

	return toLogEntries(v.(string))
}

func (s *server) commitHandler(msg maelstrom.Message) error {
	var req offsetReq
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	for key, offset := range req.Offsets {
		if err := s.kv.Write(context.Background(), fmt.Sprintf("%s%s", prefixCommit, key), offset); err != nil {
			return err
		}
	}

	return s.n.Reply(msg, map[string]any{
		"type": "commit_offsets_ok",
	})
}

func (s *server) listHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	res := make(map[string]int)

	keys := body["keys"].([]any)
	for _, key := range keys {
		k := key.(string)

		v, err := s.kv.ReadInt(context.Background(), fmt.Sprintf("%s%s", prefixCommit, k))
		if err != nil {
			v = 0
		}

		res[k] = v
	}

	return s.n.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": res,
	})
}
