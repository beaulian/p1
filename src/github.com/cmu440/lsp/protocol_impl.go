
package lsp

import (
    "fmt"
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
    sendWindow *SyncMap
    sendWindowMax *SyncCounter
    lastAckSeqNum *SyncCounter
    // 接收窗口
    recvWindow *SyncMap
    recvWindowMax *SyncCounter
    lastRecvSeqNum *SyncCounter
    recvMsgCount *SyncCounter

    nextSeqNum *SyncCounter
	// 用于提醒处理接收的消息
	handleRecvMsgChan chan *Message
    handleSendMsgChan chan *Message
    recvMsgChan chan interface{}
    toCloseChan chan int
    closeClientChan chan int
    doneCloseChan chan int
    // 当前定时器超时次数
	epochCounter *SyncCounter
	epochTimer *SyncTimer

    isLost *SyncBool
    isClose *SyncBool
    haveUnAck *SyncBool
    haveUnRecv *SyncBool

    lsp *LSP
}

type LspServer struct {
    conn *lspnet.UDPConn
	clientCount int
	clients *SyncMap
	recvMsgChan chan interface{}
	isClose *SyncBool
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
        case <- c.epochTimer.GetC():
                c.epochCounter.Inc()
				if c.epochCounter.Value() >= l.params.EpochMillis {
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
					c.epochCounter.Set(0)
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
            sendWindow: NewSyncMap(),
            sendWindowMax: NewSyncCounter(l.params.WindowSize),
            lastAckSeqNum: NewSyncCounter(0),
            recvWindow: NewSyncMap(),
            recvWindowMax: NewSyncCounter(l.params.WindowSize),
            lastRecvSeqNum: NewSyncCounter(0),
            nextSeqNum: NewSyncCounter(1),
            recvMsgCount: NewSyncCounter(0),
            // 用于提醒处理接收的消息
            handleRecvMsgChan: make(chan *Message, MAXN),
            handleSendMsgChan: make(chan *Message, MAXN),
            recvMsgChan: make(chan interface{}, MAXN),
            toCloseChan: make(chan int, 1),
            closeClientChan: make(chan int, 1),
            doneCloseChan: make(chan int, 1),
            // 当前定时器超时次数
            epochCounter: NewSyncCounter(0),
            epochTimer: NewSyncTimer(time.Millisecond * time.Duration(l.params.EpochMillis)),

            isClose: NewSyncBool(false),
            isLost: NewSyncBool(false),
            haveUnAck: NewSyncBool(false),
            haveUnRecv: NewSyncBool(false),
            lsp: l,
        }
    case doServer:
        return &LspServer{
            conn: conn,
    		clientCount: 0,
    		clients: NewSyncMap(),
    		toCloseChan: make(chan int, 1),
    		doneCloseChan: make(chan int, 1),
    		closeClientChan: make(chan int, MAXN),
    		recvMsgChan: make(chan interface{}, 1),
    		isClose: NewSyncBool(false),

            lsp: l,
        }
    }
    return nil
}

func (l *LSP) ListenLSP(lAddr *lspnet.UDPAddr) (interface{}, error) {
    fmt.Println("debug...")
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
    msg := NewData(c.connID, c.nextSeqNum.Value(), payload)
    fmt.Println("客户端发送: ", msg)
    c.nextSeqNum.Inc()
    // 通知发送消息
    c.handleSendMsgChan <- msg
    return nil
}

func (s *LspServer) Write(connID int, payload []byte) error {
    c, ok := s.clients.Get(connID).(*LspClient)
    if !ok {
        return errors.New("The connection with client has closed.")
    }
    msg := NewData(connID, c.nextSeqNum.Value(), payload)
    c.nextSeqNum.Inc()
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
        c, _ := s.clients.Get(i).(*LspClient)
        // 因为至始至终都只有一个socket，所以这里不用关连接
        c.toCloseChan <- 1
        // <-c.doneCloseChan
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
            // 先发送处理消息的信号
            c.handleRecvMsgChan <- msg
        }
    }
}

func (s *LspServer) recvMsgLoop() {
    // fmt.Println("服务器启动")
    for {
        select {
        // 所有服务器维持的客户端连接已经关闭
        case <-s.toCloseChan:
            s.isClose.Set(true)
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
			if s.isClose.Value() && s.clients.Len() == 0 {
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
                c := s.lsp.createConn(s.conn, nil, doClient).(*LspClient)
                s.clientCount++
                c.connID = s.clientCount
                c.rAddr = rAddr
                s.clients.Set(c.connID, c)
                c.lsp.sendMsg(c, NewAck(c.connID, 0))
                // 在服务器端closeClientChan一定不能是客户端连接的
                go s.lsp.handleMsgLoop(c, s.recvMsgChan, s.closeClientChan)
            } else {
                c, ok := s.clients.Get(msg.ConnID).(*LspClient)
                if !ok {
                    continue
                }
                // 先发送处理消息的信号
                c.handleRecvMsgChan <- msg
            }
        }
    }
}

func (c *LspClient) canCloseChan() bool {
    c.isClose.Set(true)
    if len(c.handleSendMsgChan) == 0 && len(c.handleRecvMsgChan) == 0 && !c.haveUnAck.Value() && !c.haveUnRecv.Value() {
        return true
    }
    return false
}

// 核心函数，处理消息
func (l *LSP) handleMsgLoop(c *LspClient, recvMsgChan chan interface{}, closeClientChan chan int) {
    for {
        if c.isLost.Value() {
            closeClientChan <- c.connID
			return
        } else {
            select {
            case msg := <- c.handleRecvMsgChan:
                // 接收到的消息介于这两个数之间，否则直接丢弃
                // fmt.Println("接收到：", msg)
                if msg.Type == MsgData && msg.SeqNum > c.lastRecvSeqNum.Value() && msg.SeqNum <= c.recvWindowMax.Value() {
                    // 检查是否未按序到达
                    l.handleRecvMsg(c, msg, recvMsgChan)
                    // 对这次发送的数据确认
                    l.sendMsg(c, NewAck(c.connID, msg.SeqNum))
                } else if msg.Type == MsgAck && msg.SeqNum > c.lastAckSeqNum.Value() && msg.SeqNum <= c.sendWindowMax.Value() {
                    l.handleRecvMsg(c, msg, recvMsgChan)
                }
            case msg := <- c.handleSendMsgChan:
                if msg.Type == MsgData {
                    c.sendWindow.Set(msg.SeqNum, &AckMsg{isAck: false, msg: msg})
                    // 启动超时定时器
                    c.epochTimer.Reset(time.Millisecond * time.Duration(l.params.EpochMillis))
                    if msg.SeqNum > c.lastAckSeqNum.Value() && msg.SeqNum <= c.sendWindowMax.Value() {
                        l.sendMsg(c, msg)
                    }
                }
            case <- c.epochTimer.GetC():
                //   fmt.Println("触发定时器...")
                  go l.handleEpochEvent(c, recvMsgChan)
              case <- c.toCloseChan:
                  if c.canCloseChan() {
                      closeClientChan <- c.connID
					  return
                  }
            }
        }
    }
}

func (l *LSP) handleMoveWindow(c *LspClient, msg *Message) (int, int) {
    flag := false
    var i int
    var lastSeqNum int
    var windowMax int
    var window *SyncMap
    if msg.Type == MsgData {
        lastSeqNum = c.lastRecvSeqNum.Value()
        windowMax = c.recvWindowMax.Value()
        window = c.recvWindow
        // fmt.Println(window, lastSeqNum, windowMax)
    } else if msg.Type == MsgAck {
        lastSeqNum = c.lastAckSeqNum.Value()
        windowMax = c.sendWindowMax.Value()
        window = c.sendWindow
    }
    // 往左边搜索
    for i = lastSeqNum+1; i < msg.SeqNum; i++ {
        if msg.Type == MsgData {
            _, ok := window.Get(i).(*Message)
            if !ok {
                flag = true
            }
        } else if msg.Type == MsgAck {
            msg, ok := window.Get(i).(*AckMsg)
            if ok && !msg.isAck {
                flag = true
            }
        }
    }
    // 往右边搜索
    endRecvSeqNum := msg.SeqNum
    firstFlag := false
    for i = windowMax; i > msg.SeqNum; i-- {
        if msg.Type == MsgData {
            msg, ok := window.Get(i).(*Message)
            if ok && !firstFlag {
                endRecvSeqNum = msg.SeqNum
                firstFlag = true
            } else if firstFlag && !ok {
                flag = true
            }
        } else if msg.Type == MsgAck {
            msg, ok := window.Get(i).(*AckMsg)
            if ok && msg.isAck && !firstFlag {
                endRecvSeqNum = msg.msg.SeqNum
                firstFlag = true
            } else if firstFlag && ok && !msg.isAck {
                flag = true
            }
        }
    }
    if flag {
        return -1, -1
    } else {
        return endRecvSeqNum, lastSeqNum
    }
}

// 处理接收窗口中接到的消息
func (l *LSP) handleRecvMsg(c *LspClient, msg *Message, recvMsgChan chan interface{}) bool {
    //只要收到了消息就置0
    c.epochCounter.Set(0)
    var endSeqNum int
    var lastSeqNum int
    if msg.Type == MsgData {
        // fmt.Println(msg)
        c.recvMsgCount.Inc()
        c.recvWindow.Set(msg.SeqNum, msg)
        endSeqNum, lastSeqNum = l.handleMoveWindow(c, msg)
        // fmt.Println(endSeqNum, lastSeqNum)
        if endSeqNum == -1 && lastSeqNum == -1 {
            c.haveUnRecv.Set(true)
        } else {
            c.haveUnRecv.Set(false)
        }
        if endSeqNum != -1 && lastSeqNum != -1 {
            for i := lastSeqNum+1; i<= endSeqNum; i++ {
                // 发送给上层Read函数
                recvMsgChan <- c.recvWindow.Get(i).(*Message)
            }
        }
        c.recvWindowMax.Add(endSeqNum - lastSeqNum)
        c.lastRecvSeqNum.Add(endSeqNum - lastSeqNum)
    } else if msg.Type == MsgAck {
        if ackMsg, ok := c.sendWindow.Get(msg.SeqNum).(*AckMsg); ok {
            ackMsg.isAck = true
            c.sendWindow.Set(msg.SeqNum, ackMsg)
        }
        endSeqNum, lastSeqNum = l.handleMoveWindow(c, msg)
        if endSeqNum == -1 && lastSeqNum == -1 {
            c.haveUnAck.Set(true)
        } else {
            c.haveUnAck.Set(false)
        }
        c.sendWindowMax.Add(endSeqNum - lastSeqNum)
        // c.syncChan <- true
        c.lastAckSeqNum.Add(endSeqNum - lastSeqNum)
    }
    if c.isClose.Value() && len(c.handleSendMsgChan) == 0 {
        return true
    }
    return false
}

func (l *LSP) handleEpochEvent(c *LspClient, recvMsgChan chan interface{}) {
    c.epochCounter.Inc()
    // 如果超过了次数限制
    if c.epochCounter.Value() >= l.params.EpochLimit {
        c.isLost.Set(true)
        recvMsgChan <- c.connID
        return
    }
    // 如果没收到任何消息
    if c.recvMsgCount.Value() == 0 {
        l.sendMsg(c, NewAck(c.connID, 0))
    }
    // 重传未确认的消息
    for i := c.lastAckSeqNum.Value()+1; i <= c.sendWindowMax.Value(); i++ {
        if msg, _ := c.sendWindow.Get(i).(*AckMsg); msg != nil && !msg.isAck {
            fmt.Println("重传: ", msg.msg)
            c.handleSendMsgChan <- msg.msg
        }
    }
    // 重发最近w条接收到的消息的Ack
    if c.recvMsgCount.Value() != 0 {
        for j := c.lastRecvSeqNum.Value(); j > 0; j-- {
            l.sendMsg(c, NewAck(c.connID, j))
        }
    }
    c.epochTimer.Reset(time.Millisecond * time.Duration(l.params.EpochMillis))
	return
}
