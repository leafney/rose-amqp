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
	channel    *amqp.Channel
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

func (c *Client) NewExchange(name string) *Exchange {

	return &Exchange{
		channel: c.channel,
		Name:    name,
		Kind:    amqp.ExchangeDirect,
	}
}

func (c *Exchange) SetType(t string) *Exchange {
	c.Kind = t
	return c
}

func (c *Exchange) Do() error {
	return c.channel.ExchangeDeclare(
		c.Name,
		amqp.ExchangeDirect,
		true,
		false,
		false,
		false,
		nil,
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

	useBind      bool
	exchangeName string
	routingKey   string

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
		exchangeName:  "",
		routingKey:    "",
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

func (c *Queue) BindExchange(name, routingKey string) *Queue {
	c.useBind = true
	c.exchangeName = name
	c.routingKey = routingKey
	return c
}

func (c *Queue) Do() (queue *Queue, err error) {

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

	if c.useQos {
		err = c.channel.Qos(c.prefetchCount, 0, c.global)
		if err != nil {
			return nil, err
		}
	}

	if c.useBind {
		err = c.channel.QueueBind(
			q.Name,
			c.routingKey,
			c.exchangeName,
			false,
			nil,
		)
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

func (c *Queue) Publish(ctx context.Context, body string) error {

	return c.channel.PublishWithContext(
		ctx,
		c.exchangeName,
		c.routingKey,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		},
	)
}

func (c *Queue) Consume(handler func(delivery amqp.Delivery)) error {

	msgs, err := c.channel.Consume(
		c.queueName,
		"",    // consumer 消费者标识
		false, // autoAck 是否自动应答
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
