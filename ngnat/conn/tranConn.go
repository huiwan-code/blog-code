package conn

import "net"

type TranConn struct {}
var maxConnId string
type Listener struct {
	net.Addr
	Conns chan *TranConn
}

func Listen(addr string) (*Listener, error) {}

/**
1. 创建连接
2. 
*/
func Dial(addr string) (*TranConn, error) {}

