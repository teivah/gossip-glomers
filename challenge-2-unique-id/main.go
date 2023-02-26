package main

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	s := &server{n: n}

	n.Handle("generate", s.run)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type server struct {
	n *maelstrom.Node
}

func (s *server) run(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var randomNum int64
	err := binary.Read(rand.Reader, binary.BigEndian, &randomNum)
	if err != nil {
		return err
	}

	response := map[string]any{
		"type": "generate_ok",
		"id":   fmt.Sprintf("%v%v", time.Now().UnixNano(), randomNum),
	}

	return s.n.Reply(msg, response)
}
