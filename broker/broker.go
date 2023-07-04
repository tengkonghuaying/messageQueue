package broker

import (
	"errors"
	"sync"
	"time"
)

type Broker struct {
	mutex    sync.RWMutex
	exit     chan bool
	capacity int
	topics   map[string][]chan Msg
}

type Msg struct {
	//根据协议增加
	Content []byte
}

func NewBroker() *Broker {
	return &Broker{}
}

// Publish 消息推送 topic : 主题  , m : 需要传递的消息
func (b *Broker) Publish(topic string, m Msg) error {
	select {
	case <-b.exit:
		return errors.New("broker closed")
	default:
	}

	b.mutex.RLock()
	subscribers, ok := b.topics[topic]
	b.mutex.RUnlock()

	if !ok {
		return nil
	}

	b.Broadcast(m, subscribers)
	return nil
}

// Broadcast 消息广播 将推送的消息发送给每一个订阅者
func (b *Broker) Broadcast(m Msg, subscribers []chan Msg) {
	count := len(subscribers)
	concurrency := 1

	switch {
	case count > 1000:
		concurrency = 3
	case count > 100:
		concurrency = 2
	default:
		concurrency = 1
	}
	pub := func(start int) {
		for j := start; j < count; j += concurrency {
			select {
			case subscribers[j] <- m:
			case <-time.After(time.Millisecond * 5):
			case <-b.exit:
				return
			}
		}
	}
	for i := 0; i < concurrency; i++ {
		go pub(i)
	}
}

// Subscribe 消息订阅 传入想要订阅的主题，返回用来接收数据的channel
func (b *Broker) Subscribe(topic string) (<-chan Msg, error) {
	select {
	case <-b.exit:
		return nil, errors.New("broker closed")
	default:
	}

	res := make(chan Msg, b.capacity)
	b.mutex.Lock()
	b.topics[topic] = append(b.topics[topic], res)
	b.mutex.Unlock()
	return res, nil
}

// Unsubscribe 取消订阅
func (b *Broker) Unsubscribe(topic string, sub <-chan Msg) error {
	select {
	case <-b.exit:
		return errors.New("broker closed")
	default:
	}

	b.mutex.RLock()
	subscribers, ok := b.topics[topic]
	b.mutex.RUnlock()

	if !ok {
		return nil
	}

	var newSubs []chan Msg
	for _, subscriber := range subscribers {
		if subscriber == sub {
			continue
		}
		newSubs = append(newSubs, subscriber)
	}

	b.mutex.Lock()
	b.topics[topic] = newSubs
	b.mutex.Unlock()
	return nil
}

// Close 关闭整个消息队列
func (b *Broker) Close() {
	select {
	case <-b.exit:
		return
	default:
		close(b.exit)
		b.mutex.Lock()
		b.topics = make(map[string][]chan Msg)
		b.mutex.Unlock()
	}
	return
}

// SetConditions 设置消息队列容量
func (b *Broker) SetConditions(capacity int) {
	b.capacity = capacity
}
