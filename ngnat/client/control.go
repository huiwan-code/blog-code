package client

import "ngnat/conn"

type Controller struct {
	wsConn    	*conn.ClientWsConn
	config 		Configuration
}

func NewController(config Configuration) (c *Controller) {
	wc := conn.NewClientWsConn(config.ServerAddr, config.Token)
	c = &Controller{
		config: config,
		wsConn: wc,
	}
	return c
}

func (c *Controller) Run() {
	defer func () {
		c.Close()
	}()
	for {
		select {
		case <- c.wsConn.Recv:

		}
	}
}