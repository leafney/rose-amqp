/**
 * @Author:      leafney
 * @GitHub:      https://github.com/leafney
 * @Project:     rose-amqp
 * @Date:        2023-12-24 10:32
 * @Description:
 */

package ramqp

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

/*
// TODO 第三版
type Channel struct {
	channel            *amqp.Channel
	Name               string
	channelNotifyClose chan *amqp.Error
	exchangeName       string

	queueName string
}

var (
	channelPool = make(map[string]*Channel)
)

func (c *Client) NewChannel(name string) (*Channel, error) {
	if ch, ok := channelPool[name]; ok {
		return ch, nil
	}

	newCh, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}

	theC := &Channel{
		Name:               name,
		channel:            newCh,
		channelNotifyClose: make(chan *amqp.Error),
	}

	newCh.NotifyClose(theC.channelNotifyClose)

	return theC, err
}

func (c *Client) DefChannel() (*Channel, error) {
	return c.NewChannel("default")
}

*/

type Exchange struct {
	channel      *amqp.Channel
	exchangeName string
	kind         string
	durable      bool
	autoDelete   bool
	Internal     bool
	NoWait       bool
	Args         amqp.Table

	routingKey string
	queueName  string
	noExchange bool
}

type EKind int

const (
	// KindEmpty default empty
	KindEmpty EKind = iota
	// KindHeaders headers
	KindHeaders
	// KindFanout fanout
	KindFanout
	// KindTopic topic
	KindTopic
	// KindDirect direct
	KindDirect
)

func (c *Client) NewExchange(name string) *Exchange {

	return &Exchange{
		channel:      c.channel,
		exchangeName: name,
		//kind:         amqp.ExchangeDirect,
		kind:       "",
		routingKey: "",
		queueName:  "",
		noExchange: false,
	}
}

func (c *Client) NoExchangeOnlyQueue(name string) *Exchange {
	e := c.NewExchange("")
	e.queueName = name
	e.noExchange = true
	return e
}

func (c *Exchange) SetKind(t EKind) *Exchange {
	var _t = ""
	switch t {
	case KindFanout:
		_t = amqp.ExchangeFanout
	case KindHeaders:
		_t = amqp.ExchangeHeaders
	case KindTopic:
		_t = amqp.ExchangeTopic
	case KindDirect:
		_t = amqp.ExchangeDirect
	case KindEmpty:
		_t = amqp.DefaultExchange
	default:
		_t = amqp.ExchangeDirect
	}

	c.kind = _t
	return c
}

func (c *Exchange) SetRoutingKey(key string) *Exchange {
	c.routingKey = key
	return c
}

func (c *Exchange) Do() (exchange *Exchange, err error) {
	//if rose.StrIsEmpty(c.exchangeName) {
	if c.noExchange {
		log.Printf("exchange Do noExchange queue [%v]", c.queueName)
		//	name empty so routingKey equal queueName
		q, err := c.channel.QueueDeclare(
			c.queueName,
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return nil, err
		}

		c.routingKey = q.Name

	} else {

		log.Printf("ExchangeDeclare ename [%v] kind [%v]", c.exchangeName, c.kind)

		err = c.channel.ExchangeDeclare(
			c.exchangeName,
			c.kind,
			true,
			false,
			false,
			false,
			nil,
		)
	}

	return c, err
}

func (c *Exchange) Publish(ctx context.Context, body string) error {

	log.Printf("publish eName [%v] key [%v]", c.exchangeName, c.routingKey)

	return c.channel.PublishWithContext(
		ctx,
		c.exchangeName,
		c.routingKey,
		false,
		false,
		amqp.Publishing{
			//DeliveryMode: amqp.Persistent,
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
}

type Queue struct {
	channel    *amqp.Channel
	queueName  string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table

	useBind  bool
	exchange *Exchange

	useQos        bool
	prefetchCount int
	global        bool
}

func (c *Client) NewQueue(name string) *Queue {
	return &Queue{
		channel:    c.channel,
		queueName:  name,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,

		useBind:       false,
		useQos:        false,
		prefetchCount: 0,
		global:        false,
	}
}

func (c *Client) DefQueue() *Queue {
	return c.NewQueue("")
}

func (c *Queue) SetDurable(durable bool) *Queue {
	c.Durable = durable
	return c
}

func (c *Queue) SetQos(count int, global bool) *Queue {
	c.useQos = true
	c.prefetchCount = count
	c.global = global
	return c
}

func (c *Queue) BindExchange(exchange *Exchange) *Queue {
	c.useBind = true
	c.exchange = exchange
	return c
}

func (c *Queue) Do() (queue *Queue, err error) {

	log.Printf("queue Do queueName [%v]", c.queueName)

	q, err := c.channel.QueueDeclare(
		c.queueName,
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// important：Redefine the queue name
	c.queueName = q.Name

	if c.useQos {
		err = c.channel.Qos(c.prefetchCount, 0, c.global)
		if err != nil {
			return nil, err
		}
	}

	if c.useBind {

		log.Printf("key [%v] ename [%v] queueName [%v]", c.exchange.routingKey, c.exchange.exchangeName, q.Name)

		err = c.channel.QueueBind(
			q.Name,
			//c.exchange.routingKey,
			"",
			c.exchange.exchangeName,
			false,
			nil,
		)
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

func (c *Queue) Consume(handler func(delivery amqp.Delivery)) error {

	msgs, err := c.channel.Consume(
		c.queueName,
		"",    // consumer 消费者标识
		true,  // autoAck 是否自动应答
		false, // exclusive 是否独占
		false, // noLocal
		false, // noWait 是否阻塞
		nil,   // args
	)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			handler(msg)
		}
	}()

	return nil
}
