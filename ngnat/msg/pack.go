package msg

import (
	"encoding/json"
	"fmt"
	"reflect"
)

type Message interface {}

func Pack(data Message) (packedMsg []byte, err error) {
	defer func () {
		if r := recover(); r != nil {
			err = fmt.Errorf("pack msg error: %v", r)
		}
	}()
	packedMsg, err = json.Marshal(struct{
		Type		string
		Payload		interface{}
	}{
		Type: reflect.TypeOf(data).Elem().Name(),
		Payload: data,
	})
	return
}

func Unpack(buf []byte) (msg Message, err error) {
	defer func () {
		if r := recover(); r != nil {
			err = fmt.Errorf("unpack msg error: %v", r)
		}
	}()
	var unpackMsg = struct {
		Type 		string
		Payload		json.RawMessage
	}{}
	err = json.Unmarshal(buf, unpackMsg)
	if err != nil {
		return
	}
	t, ok := TypeMsg[unpackMsg.Type]
	if !ok {
		err = fmt.Errorf("Unsupported message type %s", unpackMsg.Type)
	}
	msg = reflect.New(t).Interface().(Message)
	json.Unmarshal(unpackMsg.Payload, &msg)
	return
}