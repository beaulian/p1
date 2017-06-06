// Contains the implementation of a LSP client.

package lsp

type client struct {
	cli *LspClient
}

func NewClient(hostport string, params *Params) (*client, error) {
	lsp := NewLSP(params, false)
	conn, err := lsp.DialLSP(hostport)
	if err != nil {
		return nil, err
	}

	cli := &client{conn.(*LspClient)}

	return cli, nil
}

func (c *client) ConnID() int {
	return c.cli.connID
}

func (c *client) Read() ([]byte, error) {
	if payload, err := c.cli.Read(); err != nil {
		return nil, err
	} else {
		return payload, nil
	}
}

func (c *client) Write(payload []byte) error {
	return c.cli.Write(payload)
}

func (c *client) Close() error {
	return c.cli.Close()
}
