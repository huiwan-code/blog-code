package conn

import (
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"net"
	msg "ngnat/msg"
	"ngnat/utils"
	"time"
)

var (
	maxPongLatency = 15 * time.Second
	keepaliveInterval = 5 * time.Second
	keepaliveTimeout = 10 * time.Second
	writeWait = 1 * time.Second
	waitForAuth = 10 * time.Second
)

type WsConn struct {
	Conn 				*websocket.Conn
	Recv				chan msg.Message
	Send				chan msg.Message
	Shutdown			chan struct{}
}

func (wc *WsConn) Close () {
	wc.Conn.Close()
}

func (wc *WsConn) reader () {
	defer utils.PanicToError()
	defer func () {
		close(wc.Shutdown)
		close(wc.Recv)
	}()
	for {
		data, err := wc.readMessage()
		if err != nil {
			if e, ok := err.(net.Error); ok && !e.Temporary() {
				return
			}
			continue
		}
		wc.Recv <- data
	}
}

func (wc *WsConn) writer() {
	defer utils.PanicToError()
	for data := range wc.Send {
		buf, err := msg.Pack(data)
		if err !=nil {
			continue
		}
		err = wc.Conn.WriteMessage(websocket.TextMessage, buf)
		if e, ok := err.(net.Error); ok && !e.Temporary() {
			// TODO: add log
			close(wc.Shutdown)
			fmt.Println(err)
			return
		}
	}
}

func (wc *WsConn) readMessage() (data interface{}, err error){
	_, readBuf, err := wc.Conn.ReadMessage()
	if err != nil {
		return
	}
	data, err = msg.Unpack(readBuf)
	return
}

func (wc *WsConn) closeHandler(code int, text string) error {
	message := websocket.FormatCloseMessage(code, "")
	wc.Conn.WriteControl(websocket.CloseMessage, message, time.Now().Add(writeWait))
	wc.Shutdown <- struct{}{}
	return nil
}


type ServerWsConn struct {
	WsConn
	Token 				string
	lastPingAccept		time.Time
}

func NewServerWsConn (conn *websocket.Conn) (wc *ServerWsConn, err error) {
	wc = new(ServerWsConn)
	wc.Conn = conn
	// auth
	token, err := wc.getAndCheckToken()
	if err != nil {
		conn.Close()
		// TODO: add log
		return
	}
	wc.Token = token
	wc.lastPingAccept = time.Now()
	wc.Recv = make(chan msg.Message)
	wc.Send = make(chan msg.Message)
	wc.Shutdown = make(chan struct{})
	conn.SetPingHandler(wc.pingHandler)
	conn.SetCloseHandler(wc.closeHandler)
	go wc.heartbeat()
	go wc.reader()
	go wc.writer()
	return
}

func (wc *ServerWsConn) getAndCheckToken() (token string, err error) {
	wc.Conn.SetReadDeadline(time.Now().Add(waitForAuth))
	data, err := wc.readMessage()
	if err != nil {
		return
	}
	wc.Conn.SetReadDeadline(time.Time{})

	if val, ok := data.(*msg.AuthReq); ok {
		token = val.Token
		// TODO: check token
		wc.Conn.SetWriteDeadline(time.Now().Add(writeWait))
		writeBuf, _ := msg.Pack(msg.AuthResp{})
		err = wc.Conn.WriteMessage(websocket.TextMessage, writeBuf)
		if err != nil {
			return
		}
		wc.Conn.SetWriteDeadline(time.Time{})
	} else {
		err = errors.New("recv wrong data  when check token")
		return
	}
	return
}


func (wc *ServerWsConn) pingHandler (message string) error {
	fmt.Println("get a ping msg")
	err := wc.Conn.WriteControl(websocket.PongMessage, []byte(message), time.Now().Add(writeWait))
	if err != nil && err != websocket.ErrCloseSent {
		if e, ok := err.(net.Error); !ok || !e.Temporary() {
			return err
		}
	}
	wc.lastPingAccept = time.Now()
	return nil
}

func (wc *ServerWsConn) heartbeat() {
	pongCheck := time.NewTicker(keepaliveTimeout)
	defer func(){
		pongCheck.Stop()
	}()
	for {
		select {
		case <- wc.Shutdown:
			return
		case <- pongCheck.C:
			if time.Now().Sub(wc.lastPingAccept) > keepaliveTimeout {
				// TODO: add log
				fmt.Println("heartbeat timeout")
				close(wc.Shutdown)
				return
			}
		}
	}
}

type ClientWsConn struct {
	WsConn
	lastPingSend		time.Time
	lastPongRecv		time.Time
}

func NewClientWsConn (serverAddr string, token string) (wc *ClientWsConn){
	var dialer websocket.Dialer
	conn, _, err := dialer.Dial(serverAddr, nil)
	if err != nil {
		panic(err)
	}
	wc = new(ClientWsConn)
	wc.Conn = conn
	err = wc.verifyToken(token)
	if err != nil {
		conn.Close()
		panic(err)
		return
	}
	wc.lastPingSend = time.Now()
	wc.lastPongRecv = time.Now()
	wc.Recv = make(chan msg.Message)
	wc.Send = make(chan msg.Message)
	wc.Shutdown = make(chan struct{})
	conn.SetCloseHandler(wc.closeHandler)
	conn.SetPongHandler(wc.pongHandler)
	go wc.heartbeat()
	return
}

func (wc *ClientWsConn) heartbeat() {
	pingTicker := time.NewTicker(keepaliveInterval)
	pongCheckTicker := time.NewTicker(time.Second)
	defer func () {
		pingTicker.Stop()
		pongCheckTicker.Stop()
	}()
	for {
		select {
		case <- pongCheckTicker.C:
			lastPingLatency := time.Since(wc.lastPingSend)
			pongAfterPing := wc.lastPongRecv.Sub(wc.lastPingSend) < 0
			if lastPingLatency > maxPongLatency && pongAfterPing {
				// TODO: add log
				close(wc.Shutdown)
				return
			}
		case <- wc.Shutdown:
			return
		case <- pingTicker.C:
			pingBuf, _ := msg.Pack(msg.PingMsg{})
			err := wc.Conn.WriteControl(websocket.PingMessage, pingBuf, time.Now().Add(writeWait))
			if e, ok := err.(net.Error); ok && !e.Temporary() {
				// TODO: add log
				close(wc.Shutdown)
				fmt.Println(err)
				return
			}
		}
	}
}
func (wc *ClientWsConn) verifyToken (token string) (err error){
	wc.Conn.SetWriteDeadline(time.Now().Add(writeWait))
	writeBuf, _ := msg.Pack(msg.AuthReq{Token: token})
	err = wc.Conn.WriteMessage(websocket.TextMessage, writeBuf)
	if err != nil {
		return
	}
	wc.Conn.SetWriteDeadline(time.Time{})
	wc.Conn.SetReadDeadline(time.Now().Add(waitForAuth))
	readData, err := wc.readMessage()
	if err != nil {
		return
	}
	if resp, ok := readData.(*msg.AuthResp); ok {
		if resp.Error == nil {
			wc.Conn.SetReadDeadline(time.Time{})
		}
		return resp.Error
	}
	return errors.New("recv wrong type data when verify token")
}

func (wc *ClientWsConn) pongHandler (message string) (err error) {
	wc.lastPongRecv = time.Now()
	return nil
}


