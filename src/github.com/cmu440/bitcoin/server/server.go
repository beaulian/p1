package main

import (
	"fmt"
	"os"
	"math"
	"strconv"
	"encoding/json"

	"github.com/cmu440/lsp"
	"github.com/cmu440/bitcoin"
)

const (
	minerRate = 10000
)

type request struct {
	msg *bitcoin.Message
	clientId int
}

type result struct {
	msg *bitcoin.Message
	minerId int
}

type client struct {
	connId int
	minHash uint64
	nonce uint64
	doing int
}

type miner struct {
	connId int
	job *request
}

type Server struct {
	server lsp.Server
	// clients
	clients map[int]*client
	// miners
	miners *lsp.SyncMap
	// tasks buffer
	tasks *lsp.Queue
	// freeMiner
	freeMiners *lsp.Queue

	requestToMinerChan chan bool
}

func NewServer(port int, server lsp.Server) (*Server, error) {
	s := &Server {
		server: server,
		clients: make(map[int]*client),
		miners: lsp.NewSyncMap(),
		tasks: lsp.NewQueue(),
		freeMiners: lsp.NewQueue(),
		requestToMinerChan: make(chan bool),
	}
	return s, nil
}

func (s *Server) handleMsgLoop() {
	for {
		id, payload, err := s.server.Read()
		if err != nil {
			if _, ok := s.clients[id]; ok {
				delete(s.clients, id)
				s.server.CloseConn(id)
			} else {
				// miner lost
				if temp, ok := s.miners.Get(id).(*miner); temp != nil && ok {
					// 找其他miner再做
					if temp.job != nil {
						s.tasks.Push(temp.job)
						s.requestToMinerChan <- true
					}
					s.miners.Remove(id)
				}
			}
		} else {
			var msg *bitcoin.Message
			err := json.Unmarshal(payload, &msg)
			if err != nil {
				break
			}
			if msg.Type == bitcoin.Request {
				newClient := &client{
					connId: id,
					minHash: ^uint64(0),  // 表示取反,取最大数
					nonce: uint64(0),
					doing: 0,
				}
				s.clients[newClient.connId] = newClient
				s.splitRequest(&request{msg, id})
				s.requestToMinerChan <- true
			} else if msg.Type == bitcoin.Result {
				miner, _ := s.miners.Get(id).(*miner)
				s.freeMiners.Push(id)
				s.requestToMinerChan <- true
				c := s.clients[miner.job.clientId]
				if c == nil {
					continue
				}
				if c.minHash > msg.Hash {
					c.minHash = msg.Hash
					c.nonce = msg.Nonce
				}
				c.doing--
				if c.doing == 0 {
					msg := bitcoin.NewResult(c.minHash, c.nonce)
					payload, err := json.Marshal(msg)
					if err != nil {
						break
					}
					s.server.Write(c.connId, payload)
					delete(s.clients, c.connId)
					s.server.CloseConn(c.connId)
				}
			} else if msg.Type == bitcoin.Join {
				newMiner := &miner{id, nil}
				s.miners.Set(id, newMiner)
				s.freeMiners.Push(id)
				s.requestToMinerChan <- true
			}
		}

	}
}

func (s *Server) handleScheduler() {
	for {
		select {
		case <- s.requestToMinerChan:
			// 找到一个可以用的miner
			f := s.freeMiners.Pop()
			// 如果空闲miners中一个都没有
			if f == nil {
				continue
			}
			minerId := f.Value.(int)
			if miner, ok := s.miners.Get(minerId).(*miner); ok && miner != nil {
				req := s.tasks.Pop()
				if req == nil {
					continue
				}
				s.tasks.Remove(req)
				s.freeMiners.Remove(f)
				job := req.Value.(*request)
				miner.job = job
				payload, err := json.Marshal(job.msg)
				if err != nil {
					continue
				}
				// write to the miner
				err = s.server.Write(minerId, payload)
				if err != nil {
					s.miners.Remove(minerId)
					// 直接关闭miner
					s.server.CloseConn(minerId)
				}
			}
		}
	}
}

func (s *Server) splitRequest(req *request) {
	client := s.clients[req.clientId]
	upper := req.msg.Upper
	lower := req.msg.Lower
	data := req.msg.Data

	if upper - lower < minerRate {
		s.tasks.Push(req)
		client.doing = 1
		return
	} else {
		count := int(math.Ceil(float64(upper-lower+1) / minerRate))
		for i := 0; i < count; i++ {
			msg := bitcoin.NewRequest(data, lower+uint64(i)*minerRate, lower+uint64(i+1)*minerRate)
			t := &request{msg, req.clientId}
			if i == count - 1 {
				t.msg.Upper = upper
			}
			s.tasks.Push(t)
		}
		client.doing = count
	}
}

func (s *Server) run() {
	go s.handleScheduler()
	s.handleMsgLoop()
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./server <port>")
		return
	}
	port, _ := strconv.Atoi(os.Args[1])
	params := lsp.NewParams()
	server, err := lsp.NewServer(port, params)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer server.Close()
	svr, err := NewServer(port, server)
	if err != nil {
		fmt.Println(err)
		return
	}
	svr.run()
}
