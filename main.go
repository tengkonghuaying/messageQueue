package main

import (
	"context"
	"mq/protocol"
	"mq/server"
	"os"
	"os/signal"
)

func main() {
	var (
		bindAddressProducer = ""
		bindAddressConsumer = ""
		producerPort        = ""
		consumerPort        = ""
	)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	ctx, fn := context.WithCancel(context.Background())
	go func() {
		<-signalChan
		fn()
	}()

	go server.TcpServer(ctx, bindAddressConsumer, consumerPort)
	server.TcpServer(ctx, bindAddressProducer, producerPort)

	protocol.Close()
}
