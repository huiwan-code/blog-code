package utils

import "fmt"

func PanicToError () {
	if err := recover(); err != nil {
		// TODO: add log
		fmt.Println(err)
	}
}
