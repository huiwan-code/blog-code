package msg

import (
	"reflect"
)

var (
	TypeMsg = make(map[string]reflect.Type)
)

func init() {
	t := func(obj interface{})(reflect.Type) {
		return reflect.TypeOf(obj).Elem()
	}
	TypeMsg["ReqTunnel"] = t((*ReqTunnel)(nil))
	TypeMsg["AuthReq"] = t((*AuthReq)(nil))
	TypeMsg["AuthResp"] = t((*AuthResp)(nil))
}

type ReqTunnel struct {
	Protocol  		string
	Subdomain 		string
}

type AuthReq struct {
	Token			string
}

type AuthResp struct {
	Error 			error
}
type PingMsg struct {}