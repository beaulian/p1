// Contains the implementation of a LSP client.

package lsp

type client struct {
	conn *LSPConn
	lsp	*LSP
}

func NewClient(hostport string, params *Params) (*client, error) {
	lsp := NewLSP(params, false)
	conn, err := lsp.DialLSP(hostport)
	if err != nil {
		return nil, err
	}

	cli := &client{conn, lsp}

	return cli, nil
}

func (c *client) ConnID() int {
	return c.conn.connID
}

func (c *client) Read() ([]byte, error) {
	if _, payload, err := c.lsp.Read(c.conn); err != nil {
		return nil, err
	} else {
		return payload, nil
	}
}

func (c *client) Write(payload []byte) error {
	return c.lsp.Write(c.conn.connID, payload)
}

func (c *client) Close() error {
	return c.lsp.CloseAll()
}
