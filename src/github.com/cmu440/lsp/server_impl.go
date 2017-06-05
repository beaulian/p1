// Contains the implementation of a LSP server.

package lsp

import (
	"strconv"

	"github.com/cmu440/lspnet"
)

type server struct {
	conn *LSPConn
	lsp	*LSP
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (*server, error) {
	listenAddr, err := lspnet.ResolveUDPAddr("udp", ":" + strconv.Itoa(port))
	if err != nil {
		return nil, err
	}

	lsp := NewLSP(params, true)
	conn, err := lsp.ListenLSP(listenAddr)
	if err != nil {
		return nil, err
	}

	svr := &server{conn, lsp}

	return svr, nil
}

func (s *server) Read() (int, []byte, error) {
	//TODO lsp.WaitForResponse() should return connID
	if connID, payload, err := s.lsp.Read(s.conn); err != nil {
		return connID, nil, err
	} else {
		return connID, payload, nil
	}
}

func (s *server) Write(connID int, payload []byte) error {
	return s.lsp.Write(connID, payload)
}

func (s *server) CloseConn(connID int) error {
	return s.lsp.Close(connID)
}

func (s *server) Close() error {
	return s.lsp.CloseAll()
}
