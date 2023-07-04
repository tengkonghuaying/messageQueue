package protocol

import (
	"bufio"
	"bytes"
	"context"
	"log"
	"mq/broker"
	"strings"
)

type Protocol struct {
	channel <-chan broker.Msg
}

var b = broker.NewBroker()

// IOLoop 逐行读取数据并将参数交给Execute处理
func (p *Protocol) IOLoop(ctx context.Context, client StatefulReadWriter) error {
	var (
		err  error
		line string
		resp []byte
	)

	client.SetState(ClientInit)

	reader := bufio.NewReader(client)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.Replace(line, "\n", "", -1)
		line = strings.Replace(line, "\r", "", -1)
		// 根据协议调整
		params := strings.Split(line, " ")

		log.Printf("PROTOCOL: %#v", params)

		resp, err = p.Execute(client, params...)
		if err != nil {
			_, err = client.Write([]byte(err.Error()))
			if err != nil {
				break
			}
			continue
		}

		if resp != nil {
			_, err = client.Write(resp)
			if err != nil {
				break
			}
		}
	}

	return err
}

// Execute 判断请求参数并调用相应的函数
func (p *Protocol) Execute(client StatefulReadWriter, params ...string) ([]byte, error) {
	var (
		err  error
		resp []byte
	)

	cmd := strings.ToUpper(params[0])

	switch cmd {
	case "SUB":
		resp, err = p.SUB(client, params)
		return resp, err
	case "GET":
		resp, err = p.GET(client)
		return resp, err
	case "FIN":
		resp, err = p.FIN(client)
		return resp, err
	case "PUB":
		resp, err = p.PUB(client, params)
		return resp, err
	}

	return nil, ClientErrInvalid
}

// SUB 为客户端绑定对应的channel
func (p *Protocol) SUB(client StatefulReadWriter, params []string) ([]byte, error) {
	if client.GetState() != ClientInit {
		return nil, ClientErrInvalid
	}

	if len(params) < 2 {
		return nil, ClientErrInvalid
	}

	topic := params[1]
	if len(topic) == 0 {
		return nil, ClientErrBadTopic
	}

	client.SetState(ClientWaitGet)

	channel, err := b.Subscribe(topic)
	if err != nil {
		return nil, err
	}
	p.channel = channel

	return nil, nil
}

// GET 从绑定的channel中读取数据
func (p *Protocol) GET(client StatefulReadWriter) ([]byte, error) {
	if client.GetState() != ClientWaitGet {
		return nil, ClientErrInvalid
	}

	msg := <-p.channel
	if msg.Content == nil {
		log.Printf("ERROR: msg == nil")
		return nil, ClientErrBadMessage
	}
	client.SetState(ClientWaitResponse)

	return msg.Content, nil
}

// FIN 收到确认后调整客户端状态，可以继续获取数据
func (p *Protocol) FIN(client StatefulReadWriter) ([]byte, error) {
	if client.GetState() != ClientWaitResponse {
		return nil, ClientErrInvalid
	}

	client.SetState(ClientWaitGet)

	return nil, nil
}

// PUB 发布消息
func (p *Protocol) PUB(client StatefulReadWriter, params []string) ([]byte, error) {
	var buf bytes.Buffer
	var err error

	if len(params) < 3 {
		return nil, ClientErrInvalid
	}

	topicName := params[1]
	body := []byte(params[2])

	_, err = buf.Write(body)
	if err != nil {
		return nil, err
	}

	err = b.Publish(topicName, broker.Msg{Content: buf.Bytes()})
	if err != nil {
		return nil, err
	}
	return []byte("OK"), nil
}

// Close 关闭消息队列
func Close() {
	b.Close()
}
