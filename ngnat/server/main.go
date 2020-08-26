package server
import (
	"fmt"
	"github.com/gorilla/websocket"
	uuid "github.com/satori/go.uuid"
	"net/http"
	"ngnat/conn"
)

var (
	controlRegistry *ControlRegistry
)


func ctlListener(addr string) {
	http.HandleFunc("/ws", wsConnect)
	http.ListenAndServe(addr, nil)
}

func wsConnect(res http.ResponseWriter, req *http.Request) {
	var upgrader = websocket.Upgrader{}
	rawConn, err := upgrader.Upgrade(res, req, nil)

	if err != nil {
		fmt.Println(err)
		return
	}
	wsConn, err := conn.NewServerWsConn(rawConn)
	if err != nil {
		return
	}
	if err := controlRegistry.addControl(wsConn); err != nil {
		return
	}
}

func Main(addr string){

	controlRegistry = &ControlRegistry{
		controls: make(map[uuid.UUID]*Control),
	}
	ctlListener(addr)

}

