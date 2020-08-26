package server

import (
	"fmt"
	"ngnat/conn"
	"ngnat/msg"
	"sync"
)

var (
	genClientIdMaxTimes = 5
)
type ControlRegistry struct {
	sync.RWMutex
	controls map[string]*Control
	unregister chan *Control
}

type Control struct {
	clientId 			string
	wsConn		 		*conn.ServerWsConn
	shutdown			chan struct{}
}


func (cr *ControlRegistry) addControl (conn *conn.ServerWsConn) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic when add control to controlRegister: %v", r)
		}
	}()
	cr.Lock()
	defer cr.Unlock()
	clientId := conn.Token
	if oldCtl, ok := cr.controls[clientId]; ok {
		close(oldCtl.shutdown)
	}

	c := &Control {
		clientId: 	clientId,
		wsConn: 	conn,
		shutdown: 	make(chan struct{}),
	}
	cr.controls[clientId] = c
	go c.manager()
	return
}

// 监控wsConn状态
func (c *Control) manager() {
	defer c.Close()
	for {
		select {
		case recvMsg, ok := <- c.wsConn.Recv:
			if !ok {
				return
			}
			switch m := recvMsg.(type) {
			case *msg.ReqTunnel:
				c.addTunnel(m)
			default:
				fmt.Println("unsupport message type: %s", m)
			}
		case <- c.wsConn.Shutdown:
			return
		case <- c.shutdown:
			return
		}
	}

}

// control关闭操作
func (c *Control) Close() {
	fmt.Println("colse the control %s", c.clientId)
	c.wsConn.Close()
}

func (c *Control) addTunnel(req *msg.ReqTunnel) {

}