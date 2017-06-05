
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

type AckMsg struct {
    isAck bool
    msg *Message
}

type LSPConn struct {
    connID int
    // 远程地址
    rAddr *lspnet.UDPAddr
	conn *lspnet.UDPConn
    // 发送窗口相关数据
    // 待发送的消息队列
    tempSendBuf *Queue
    sendWindow *SyncMap
    sendWindowMax *SyncCounter
    lastAckSeqNum *SyncCounter
    // 接收窗口
    recvWindow *SyncMap
    // 暂存消息队列
    tempRecvBuf *Queue
    recvWindowMax *SyncCounter
    lastRecvSeqNum *SyncCounter
    recvMsgCount *SyncCounter

    nextSeqNum *SyncCounter
	// 用于提醒处理接收的消息
	handleRecvMsgChan chan *Message
    handleSendMsgChan chan *Message
	// readChan chan *Message
	// writeChan chan *Message
    recvMsgChan chan *Message
    canClose *SyncBool
    // 当前定时器超时次数
	epochCounter *SyncCounter
	epochTimer *time.Timer

    isLost *SyncBool

    syncChan chan bool
}

type LSP struct {
    params *Params
    connCount int
	connMap map[int]*LSPConn
    newLSPConn chan *LSPConn
    startCloseChan chan int
    readyCloseChan chan int
    doneCloseChan chan int
    // 使用该协议的是不是服务端
	isServer	bool
}

func NewLSP(params *Params, isServer bool) *LSP {
    connMap := make(map[int]*LSPConn)
    newLSPConn := make(chan *LSPConn)
    startCloseChan := make(chan int)
    readyCloseChan := make(chan int)
    doneCloseChan := make(chan int)
    return &LSP{params, 0, connMap, newLSPConn, startCloseChan,
                readyCloseChan, doneCloseChan, isServer}
}

func (l *LSP) DialLSP(hostport string) (*LSPConn, error) {

    if addr, err := lspnet.ResolveUDPAddr("udp", hostport); err != nil {
		return nil, err
	} else if conn, err := lspnet.DialUDP("udp", nil, addr); err != nil {
		return nil, err
	} else {
        c := l.createConn(conn, addr, l.params)
        // 添加到connMap中
        // l.connMap[c.connID] = c
		// send connection request to server
		if err := l.sendMsg(c, NewConnect()); err != nil {
			return nil, err
		}
		// 设置超时定时器
		c.epochTimer.Reset(time.Millisecond * time.Duration(l.params.EpochMillis))
		for {
			select {
			// 定时器超时
            case <- c.epochTimer.C:
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
                            l.connMap[c.connID] = c
                            // fmt.Println(c.connID)
                            go l.recvMsgLoop(c)
                            go l.handleMsgLoop(c, c.recvMsgChan)
							return c, nil
						}
					}
				}
			}
		}
    }
}

// 创建客户端
func (l *LSP) createConn(conn *lspnet.UDPConn, rAddr *lspnet.UDPAddr, params *Params) *LSPConn {
	c := &LSPConn{
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
    	// readChan chan *Message
    	// writeChan chan *Message
        tempSendBuf: NewQueue(),
        tempRecvBuf: NewQueue(),
        recvMsgChan: make(chan *Message, MAXN),
        canClose: NewSyncBool(false),
        // 当前定时器超时次数
    	epochCounter: NewSyncCounter(0),
    	epochTimer: time.NewTimer(time.Millisecond * time.Duration(l.params.EpochMillis)),

        isLost: NewSyncBool(false),

        syncChan: make(chan bool, 1),
	}
	return c
}

func (l *LSP) ListenLSP(lAddr *lspnet.UDPAddr) (*LSPConn, error) {
    fmt.Println("debug...")
    conn, err := lspnet.ListenUDP("udp", lAddr)
    if err != nil {
        return nil, err
    }
    s := l.createConn(conn, nil, l.params)
    l.connMap[s.connID] = s
    go l.recvMsgLoop(s)
    return s, nil
}

// 发送单个消息
func (l *LSP) sendMsg(c *LSPConn, msg *Message) error {
	writeMsg, err := json.Marshal(msg)
    if err != nil {
		return err
	}
    if l.isServer {
        if _, err1 := c.conn.WriteToUDP(writeMsg, c.rAddr); err1 != nil {
            return err1
        }
    } else {
        if _, err2 := c.conn.Write(writeMsg); err2 != nil {
            return err2
        }
    }
    // fmt.Println("发送：", msg)
	return nil
}

// 接收单个消息
func (l *LSP) recvMsg(c *LSPConn) (*Message, *lspnet.UDPAddr, error) {
    // c.conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(l.params.EpochMillis)))
	buffer := make([]byte, MAXN)
	if size, rAddr, err := c.conn.ReadFromUDP(buffer); err != nil {
		return nil, nil, err
	} else {
		var msg Message
		if err = json.Unmarshal(buffer[:size], &msg); err != nil {
			return nil, nil, err
		}
        // fmt.Println("接收：", msg)
		return &msg, rAddr, nil
	}
}

// 接收消息的事件循环
func (l *LSP) recvMsgLoop(c *LSPConn) {
    // fmt.Println(l.connMap)
    // c := l.connMap[connID]
    for {
        select {
        case connID := <- l.readyCloseChan:
            l.doneCloseChan <- connID
            close(l.connMap[connID].recvMsgChan)
            return
        default:
            msg, rAddr, err := l.recvMsg(c)
            if err != nil {
                return
            }
            // fmt.Printf("%d %s\n", c.connID, msg.Payload)
            // 保存remote address
            if l.isServer && msg.Type == MsgConnect {
                    // fmt.Println("yes")
                    // 发送Ack
                    c1 := l.createConn(c.conn, nil, l.params)
                    l.connCount++
                    c1.connID = l.connCount
                    c1.rAddr = rAddr
                    l.connMap[c1.connID] = c1
                    l.sendMsg(c1, NewAck(c1.connID, 0))
                    // go l.recvMsgLoop(c.connID)
                    go l.handleMsgLoop(c1, c.recvMsgChan)
            } else {
                // 先发送处理消息的信号
                l.connMap[msg.ConnID].handleRecvMsgChan <- msg
            }
        }
    }
}


func (l *LSP) handleSendMsg(c *LSPConn, msg *Message) {
    if msg.SeqNum <= c.sendWindowMax.Value() {
        // fmt.Println("send: ", msg)
        l.sendMsg(c, msg)
    } else if msg.SeqNum > c.lastAckSeqNum.Value() {
        c.tempSendBuf.Push(msg)
        // fmt.Println("waiting send: ", msg)
    }
}

func (l *LSP) handleMoveWindow(c *LSPConn, msg *Message) int {
    flag := false
    var i int
    var lastSeqNum int
    var windowMax int
    var window *SyncMap
    if msg.Type == MsgData {
        lastSeqNum = c.lastRecvSeqNum.Value()
        windowMax = c.recvWindowMax.Value()
        window = c.recvWindow
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
        return 0
    } else {
        return (endRecvSeqNum - lastSeqNum)
    }
}

// 处理接收窗口中接到的消息
func (l *LSP) handleRecvMsg(c *LSPConn, msg *Message) {
    // fmt.Println("recv: ", msg)
    if msg.Type == MsgData {
        c.recvMsgCount.Inc()
        c.recvWindow.Set(msg.SeqNum, msg)
        count := l.handleMoveWindow(c, msg)
        c.recvWindowMax.Set(c.recvWindowMax.Value() + count)
        c.lastRecvSeqNum.Set(c.lastRecvSeqNum.Value() + count)
        c.canClose.Set(true)
    } else if msg.Type == MsgAck {
        if ackMsg, ok := c.sendWindow.Get(msg.SeqNum).(*AckMsg); ok {
            ackMsg.isAck = true
            c.sendWindow.Set(msg.SeqNum, ackMsg)
        }
        count := l.handleMoveWindow(c, msg)
        c.sendWindowMax.Set(c.sendWindowMax.Value() + count)
        // c.syncChan <- true
        c.lastAckSeqNum.Set(c.lastAckSeqNum.Value() + count)
        c.canClose.Set(true)
        if count != 0 {
            // 重发暂存队列的消息
            for e := c.tempSendBuf.Values().Front(); e != nil; e = e.Next() {
                msg := e.Value.(*Message)
        		if msg.SeqNum > c.lastAckSeqNum.Value() && msg.SeqNum <= c.sendWindowMax.Value() {
                    c.handleSendMsgChan <- msg
                    c.tempSendBuf.Remove(e)
                }
        	}
        }
    }
    return
}

func (l *LSP) handleEpochEvent(c *LSPConn) {
    c.epochCounter.Inc()
    // 如果超过了次数限制
    if c.epochCounter.Value() >= l.params.EpochLimit {
        c.isLost.Set(true)
        l.Close(c.connID)
        return
    }
    // 如果没收到任何消息
    if c.recvMsgCount.Value() == 0 {
        l.sendMsg(c, NewAck(c.connID, 0))
    }
    // 重传未确认的消息
    for i := c.lastAckSeqNum.Value()+1; i <= c.sendWindowMax.Value(); i++ {
        if msg, _ := c.sendWindow.Get(i).(*AckMsg); msg != nil && !msg.isAck {
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

func (c *LSPConn) canCloseChan() bool {
    if c.canClose.Value() && len(c.handleSendMsgChan) == 0 {
        return true
    }
    return false
}

// 核心函数，处理消息
func (l *LSP) handleMsgLoop(c *LSPConn, recvMsgChan chan *Message) {
    for {
        select {
        case msg := <- c.handleRecvMsgChan:
            // 接收到的消息介于这两个数之间，否则直接丢弃
            if msg.Type == MsgData && msg.SeqNum > c.lastRecvSeqNum.Value() && msg.SeqNum <= c.recvWindowMax.Value() {
                // 发送给上层Read函数
                recvMsgChan <- msg
                // 检查是否未按序到达
                l.handleRecvMsg(c, msg)
                // 对这次发送的数据确认
                l.sendMsg(c, NewAck(c.connID, msg.SeqNum))
            } else if msg.Type == MsgAck && msg.SeqNum > c.lastAckSeqNum.Value() && msg.SeqNum <= c.sendWindowMax.Value() {
                l.handleRecvMsg(c, msg)
            }
        case msg := <- c.handleSendMsgChan:
            if msg.Type == MsgData {
                c.sendWindow.Set(msg.SeqNum, &AckMsg{isAck: false, msg: msg})
                // 启动超时定时器
                c.epochTimer.Reset(time.Millisecond * time.Duration(l.params.EpochMillis))
                l.handleSendMsg(c, msg)
            }
        case <- c.epochTimer.C:
            //   fmt.Println("触发定时器...")
              go l.handleEpochEvent(c)
        case connID := <- l.startCloseChan:
            if l.connMap[connID].canCloseChan() {
                l.readyCloseChan <- connID
                return
            }
        }
    }
}

func (l *LSP) Read(c *LSPConn) (int, []byte, error) {
    select {
    case msg := <- c.recvMsgChan:
        return msg.ConnID, msg.Payload, nil
    }
    return -1, nil, nil
}

func (l *LSP) Write(connID int, payload []byte) error {
    c := l.connMap[connID]
	if !c.isLost.Value() {
        msg := NewData(connID, c.nextSeqNum.Value(), payload)
    	c.nextSeqNum.Inc()
    	c.handleSendMsgChan <- msg
    	return nil
    }
    return nil
}

func (l *LSP) Close(connID int) error {
	l.startCloseChan <- connID
	<-l.doneCloseChan
	l.connMap[connID].conn.Close()
	return nil
}

func (l *LSP) CloseAll() error {
	for connID, _ := range l.connMap {
        l.Close(connID)
    }
    return nil
}
