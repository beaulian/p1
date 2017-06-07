
package lsp

import (
    // "fmt"
	"time"
    "errors"
    "strings"
    "encoding/json"

    "github.com/cmu440/lspnet"
)

const MAXN = 1024

const (
    doClient = iota
    doServer
)

type AckMsg struct {
    isAck bool
    msg *Message
}

type LspClient struct {
    connID int
    // 远程地址
    rAddr *lspnet.UDPAddr
	conn *lspnet.UDPConn
    // 发送窗口相关数据
    // 发送窗口相关数据
    sendWindow map[int]interface{}
    sendWindowMax int
    lastAckSeqNum int
    // 接收窗口
    recvWindow map[int]interface{}
    recvWindowMax int
    lastRecvSeqNum int
    recvMsgCount int

    nextSeqNum int
	// 用于提醒处理接收的消息
	handleRecvMsgChan chan *Message
    handleSendMsgChan chan *Message
    recvMsgChan chan interface{}
    toCloseChan chan int
    closeClientChan chan int
    doneCloseChan chan int
    // 当前定时器超时次数
	epochCounter int
	epochTimer *time.Timer

    isLost *SyncBool
    isClose bool
    haveUnAck bool

    lsp *LSP
}

type LspServer struct {
    conn *lspnet.UDPConn
	clientCount int
	clients *SyncMap
    clientAddr map[string]*LspClient
	recvMsgChan chan interface{}
	isClose bool
	toCloseChan chan int // to notify server to close
	doneCloseChan chan int // wait server close
	closeClientChan chan int // to notify delete closed sclient

    lsp *LSP
}

type LSP struct {
    params *Params
    isServer bool
}

func NewLSP(params *Params, isServer bool) *LSP {
    return &LSP{params, isServer}
}

func (l *LSP) DialLSP(hostport string) (interface{}, error) {
    if addr, err := lspnet.ResolveUDPAddr("udp", hostport); err != nil {
		return nil, err
	} else if conn, err := lspnet.DialUDP("udp", nil, addr); err != nil {
		return nil, err
	} else {
        c, _ := l.createConn(conn, addr, doClient).(*LspClient)
		if err := l.sendMsg(c, NewConnect()); err != nil {
			return nil, err
		}
		// 设置超时定时器
		c.epochTimer.Reset(time.Millisecond * time.Duration(l.params.EpochMillis))
		for {
			select {
			// 定时器超时
            case <- c.epochTimer.C:
                c.epochCounter++
				if c.epochCounter >= l.params.EpochMillis {
					return nil, errors.New("Error: Connection cannot established.")
				}
				// 重发connection request
				if err := l.sendMsg(c, NewConnect()); err != nil {
					return nil, err
				}
                // 重新启动超时定时器
				c.epochTimer.Reset(time.Millisecond * time.Duration(l.params.EpochMillis))
			default:
				if msg, rAddr, err := l.recvMsg(c); err != nil {
					continue
				} else {
					c.epochCounter = 0
					if strings.EqualFold(rAddr.String(), c.rAddr.String()) {
						if msg.Type == MsgAck && msg.SeqNum == 0 {
                            c.connID = msg.ConnID
                            // fmt.Println(c.connID)
                            go c.recvMsgLoop()
                            go l.handleMsgLoop(c, c.recvMsgChan, c.closeClientChan)
							return c, nil
						}
					}
				}
			}
		}
    }
}

// 创建客户端
func (l *LSP) createConn(conn *lspnet.UDPConn, rAddr *lspnet.UDPAddr, mode int) interface{} {
    switch mode {
    case doClient:
        return &LspClient{
            connID: 0,
            conn: conn,
            rAddr: rAddr,
            sendWindow: make(map[int]interface{}),
            sendWindowMax: l.params.WindowSize,
            lastAckSeqNum: 0,
            recvWindow: make(map[int]interface{}),
            recvWindowMax: l.params.WindowSize,
            lastRecvSeqNum: 0,
            nextSeqNum: 1,
            recvMsgCount: 0,
            // 用于提醒处理接收的消息
            handleRecvMsgChan: make(chan *Message, MAXN),
            handleSendMsgChan: make(chan *Message, MAXN),
            recvMsgChan: make(chan interface{}, MAXN),
            toCloseChan: make(chan int, 1),
            closeClientChan: make(chan int, 1),
            doneCloseChan: make(chan int, 1),
            // 当前定时器超时次数
            epochCounter: 0,
            epochTimer: time.NewTimer(0),

            isClose: false,
            isLost: NewSyncBool(false),
            haveUnAck: true,
            lsp: l,
        }
    case doServer:
        return &LspServer{
            conn: conn,
    		clientCount: 0,
    		clients: NewSyncMap(),
            clientAddr: make(map[string]*LspClient, 1),
    		toCloseChan: make(chan int, 1),
    		doneCloseChan: make(chan int, 1),
    		closeClientChan: make(chan int, MAXN),
    		recvMsgChan: make(chan interface{}, 1),
    		isClose: false,

            lsp: l,
        }
    }
    return nil
}

func (l *LSP) ListenLSP(lAddr *lspnet.UDPAddr) (interface{}, error) {
    // fmt.Println("debug...")
    conn, err := lspnet.ListenUDP("udp", lAddr)
    if err != nil {
        return nil, err
    }
    s := l.createConn(conn, nil, doServer).(*LspServer)
	go s.recvMsgLoop()
	return s, nil
}

// 发送单个消息
func (l *LSP) sendMsg(c *LspClient, msg *Message) error {
	writeMsg, err := json.Marshal(msg)
    if err != nil {
		return err
	}
    if l.isServer {
        if _, err := c.conn.WriteToUDP(writeMsg, c.rAddr); err != nil {
            return err
        }
    } else {
        if _, err := c.conn.Write(writeMsg); err != nil {
            return err
        }
    }
	return nil
}

// 接收单个消息
func (l *LSP) recvMsg(i interface{}) (*Message, *lspnet.UDPAddr, error) {
    buffer := make([]byte, MAXN)
    var size int
    var rAddr *lspnet.UDPAddr
    var err error
    // 之所以这样做是因为go不支持泛型，而且if语句内定义的变量隐藏在其自身作用域中
    if l.isServer {
        c := i.(*LspServer)
        // 这里是为了不让接收消息这个过程一直阻塞
        c.conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(l.params.EpochMillis)))
    	if size, rAddr, err = c.conn.ReadFromUDP(buffer); err != nil {
    		return nil, nil, err
    	}
    } else {
        c := i.(*LspClient)
        c.conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(l.params.EpochMillis)))
    	if size, rAddr, err = c.conn.ReadFromUDP(buffer); err != nil {
    		return nil, nil, err
    	}
    }
	var msg Message
	if err = json.Unmarshal(buffer[:size], &msg); err != nil {
		return nil, nil, err
	}
	return &msg, rAddr, nil

}

func (c *LspClient) Read() ([]byte, error) {
    select {
    case data, ok := <-c.recvMsgChan:
		if !ok {
			return nil, errors.New("Read error, the server has been lost.")
		}
		msg, ok := data.(*Message)
		if ok {
			return msg.Payload, nil
		} else {
			return nil, errors.New("Read fail, the server has been lost.")
		}
	}
}

func (s *LspServer) Read() (int, []byte, error) {
    // fmt.Println("客户端想要发送: ")
	for {
		select {
		case data := <-s.recvMsgChan:
            // 判断是否收到客户端连接丢失的消息
            id, ok := data.(int)
			if ok {
				return id, nil, errors.New("The server read eror, some client has been lost.")
			}
			msg, ok := data.(*Message)
			if !ok {
				continue
			} else {
                // fmt.Println("服务器接收到消息: ", msg)
				return msg.ConnID, msg.Payload, nil
			}
		}
	}
}

func (c *LspClient) Write(payload []byte) error {
    if c.isLost.Value() {
        return errors.New("Write fail. The client has lost")
    }
    msg := NewData(c.connID, c.nextSeqNum, payload)
    c.nextSeqNum++
    // fmt.Println("客户端想要发送: ", msg)
    // 通知发送消息
    c.handleSendMsgChan <- msg
    return nil
}

func (s *LspServer) Write(connID int, payload []byte) error {
    c, ok := s.clients.Get(connID).(*LspClient)
    if !ok {
        return errors.New("The connection with client has closed.")
    }
    msg := NewData(connID, c.nextSeqNum, payload)
    c.nextSeqNum++
    c.handleSendMsgChan <- msg
    return nil
}

func (c *LspClient) Close() error {
    c.toCloseChan <- 1
    <-c.doneCloseChan
    c.conn.Close()
	return nil
}

func (s *LspServer) Close() error {
    for i := 1; i <= s.clients.Len(); i++ {
        if c, ok := s.clients.Get(i).(*LspClient); ok {
            // 因为至始至终都只有一个socket，所以这里不用关连接
            c.toCloseChan <- 1
            // <-c.doneCloseChan
        }
    }
	s.toCloseChan <- 1
	<-s.doneCloseChan
	s.conn.Close()
	return nil
}

func (s *LspServer) CloseConn(connID int) error {
    c, ok := s.clients.Get(connID).(*LspClient)
    if !ok {
        return errors.New("The connecion with client has lost.")
    }
    // CloseConn函数不能阻塞
    c.toCloseChan <- 1
    return nil
}


// 接收消息的事件循环
func (c *LspClient) recvMsgLoop() {
    // fmt.Println("客户端启动")
    for {
        select {
        case <-c.closeClientChan:
			c.doneCloseChan <- 1
			close(c.recvMsgChan)
			return
        default:
            msg, _, err := c.lsp.recvMsg(c)
            if err != nil {
                continue
            }
            if msg.Type == MsgAck {
				c.handleRecvMsgChan <- msg
			} else if msg.Type == MsgData {
				ack := NewAck(msg.ConnID, msg.SeqNum)
				c.lsp.sendMsg(c, ack)
				c.handleRecvMsgChan <- msg
			}
        }
    }
}

func (s *LspServer) recvMsgLoop() {
    // fmt.Println("服务器启动")
    for {
        select {
        // 所有服务器维持的客户端连接已经关闭
        case <-s.toCloseChan:
            s.isClose = true
            // 再次确认
            if s.clients.Len() == 0 {
                // 关闭服务器最开始的连接
                s.doneCloseChan <- 1
				return
            }
        // 关闭单个客户端连接
        case connID := <-s.closeClientChan:
            s.clients.Remove(connID)
            //  检查此时是否能关闭服务器
			if s.isClose && s.clients.Len() == 0 {
                s.doneCloseChan <- 1
    			return
            }
        default:
            msg, rAddr, err := s.lsp.recvMsg(s)
            if err != nil {
                continue
            }
            // 收到客户端连接请求
            if msg.Type == MsgConnect {
                if s.isClose {
					continue
				}
                if temp, ok := s.clientAddr[rAddr.String()]; ok {
                    temp.lsp.sendMsg(temp, NewAck(temp.connID, 0))
                    continue
                }
                // fmt.Println("yes", rAddr.String())
                c := s.lsp.createConn(s.conn, nil, doClient).(*LspClient)
                s.clientCount++
                c.connID = s.clientCount
                c.rAddr = rAddr
                c.lsp.sendMsg(c, NewAck(c.connID, 0))
                s.clients.Set(c.connID, c)
                s.clientAddr[rAddr.String()] = c
                // 在服务器端closeClientChan一定不能是客户端连接的
                go s.lsp.handleMsgLoop(c, s.recvMsgChan, s.closeClientChan)
            } else {
                c, ok := s.clients.Get(msg.ConnID).(*LspClient)
                if !ok {
                    continue
                }
                if msg.Type == MsgAck {
    				c.handleRecvMsgChan <- msg
    			} else if msg.Type == MsgData {
    				ack := NewAck(msg.ConnID, msg.SeqNum)
    				c.lsp.sendMsg(c, ack)
    				c.handleRecvMsgChan <- msg
    			}
                // fmt.Println("服务器接收: ", msg)
            }
        }
    }
}

func (c *LspClient) canCloseChan() bool {
    c.isClose = true
    if len(c.handleSendMsgChan) == 0 && !c.haveUnAck {
        return true
    }
    return false
}

// 核心函数，处理消息
func (l *LSP) handleMsgLoop(c *LspClient, recvMsgChan chan interface{}, closeClientChan chan int) {
    // 启动超时定时器
    c.epochTimer.Reset(time.Millisecond * time.Duration(l.params.EpochMillis))
	for {
		if c.isLost.Value() {
			closeClientChan <- c.connID
			return
		} else {
            select {
    		case <-c.toCloseChan:
    			if c.canCloseChan() {
    				closeClientChan <- c.connID
    				return
    			}
    		case <-c.epochTimer.C:
    			l.handleEpochEvent(c, recvMsgChan)
    		case msg := <-c.handleSendMsgChan:
    			l.handleSendMsg(c, msg)
    		case msg := <-c.handleRecvMsgChan:
    			if l.handleRecvMsg(c, msg, recvMsgChan) {
    				closeClientChan <- c.connID
    				return
    			}
    		}
        }
	}
}

func (l *LSP) handleSendMsg(c *LspClient, msg *Message) {
    if _, ok := c.sendWindow[msg.SeqNum]; !ok {
        c.sendWindow[msg.SeqNum] = &AckMsg{false, msg}
    }
    l.handlePendingSendMsg(c)
}

func (l *LSP) handlePendingSendMsg(c *LspClient) {
    for i := c.lastAckSeqNum+1; i <= c.sendWindowMax; i++ {
        if ackMsg, ok := c.sendWindow[i].(*AckMsg); ok {
            l.sendMsg(c, ackMsg.msg)
        }
    }
}

// 处理接收窗口中接到的消息
func (l *LSP) handleRecvMsg(c *LspClient, msg *Message, recvMsgChan chan interface{}) bool {
    c.epochCounter = 0
    if msg.Type == MsgAck && msg.SeqNum > c.lastAckSeqNum {
        // fmt.Println("客户端收到了Ack", msg)
        if ackMsg, ok := c.sendWindow[msg.SeqNum].(*AckMsg); ok && !ackMsg.isAck {
            ackMsg.isAck = true
            c.sendWindow[msg.SeqNum] = ackMsg
        }
        var i int
        for i = c.lastAckSeqNum+1; i <= len(c.sendWindow); i++ {
            if ackMsg, ok := c.sendWindow[i].(*AckMsg); ok {
                if !ackMsg.isAck {
                    c.haveUnAck = true
                    break
                } else {
                    c.lastAckSeqNum++
                    c.sendWindowMax++
                }
            }
        }
        // 不存在失序的Ack
        if i == len(c.sendWindow) + 1 {
            c.haveUnAck = false
        }
        // 再发发送窗口中可以发的消息
        l.handlePendingSendMsg(c)
    } else if msg.Type == MsgData && msg.SeqNum > c.lastRecvSeqNum {
        c.recvMsgCount++
        if _, ok := c.recvWindow[msg.SeqNum]; !ok {
            c.recvWindow[msg.SeqNum] = msg
        }
        for j := c.lastRecvSeqNum+1; j <= c.recvWindowMax; j++ {
            if msg, ok := c.recvWindow[j]; !ok {
                break
            } else {
                // fmt.Println("服务器把消息传给了Read: ", msg)
                recvMsgChan <- msg
                c.lastRecvSeqNum++
                c.recvWindowMax++
            }
        }
    }

    if c.isClose && len(c.handleSendMsgChan) == 0 && !c.haveUnAck {
		return true
	}
	return false
}


func (l *LSP) handleEpochEvent(c *LspClient, recvMsgChan chan interface{}) {
    c.epochCounter++
    // 如果超过了次数限制
    if c.epochCounter >= l.params.EpochLimit {
        c.isLost.Set(true)
        recvMsgChan <- c.connID
        return
    }
    // 如果没收到任何消息
    if c.recvMsgCount == 0 {
        l.sendMsg(c, NewAck(c.connID, 0))
    }
    // 重传未确认的消息
    for i := c.lastAckSeqNum+1; i <= c.sendWindowMax; i++ {
        ackMsg, ok := c.sendWindow[i].(*AckMsg)
        if ok && !ackMsg.isAck {
            c.handleSendMsgChan <- ackMsg.msg
        }
    }
    // 重发最近WindowSize条接收到的消息的Ack
    if c.recvMsgCount != 0 {
        i := c.lastRecvSeqNum
        for j := l.params.WindowSize; j > 0 && i > 0; j-- {
            l.sendMsg(c, NewAck(c.connID, i))
            i--
        }
    }
    c.epochTimer.Reset(time.Millisecond * time.Duration(l.params.EpochMillis))
	return
}
