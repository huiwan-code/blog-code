package client


func Main(){
	config := Configuration{
		ServerAddr:  	"ws://127.0.0.1:7777",
		Token:			"test",
	}
	NewController(config).Run()

}