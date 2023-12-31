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
		kind:         amqp.DefaultExchange,
		routingKey:   "",
		queueName:    "",
		noExchange:   false,
	}
}

func (c *Client) NoExchangeOnlyQueue(name string) *Exchange {
	e := c.NewExchange("")
	e.noExchange = true
	e.queueName = name
	return e
}

func (e *Exchange) SetKind(t EKind) *Exchange {
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

	e.kind = _t
	return e
}

// TODO: 考虑区分一下，在 exchange初始化时设置和在publish时设置的方法名最好区分一下
func (e *Exchange) SetRoutingKey(key string) *Exchange {
	e.routingKey = key
	return e
}

func (e *Exchange) SetDurable(durable bool) *Exchange {
	e.durable = durable
	return e
}

func (e *Exchange) Do() (exchange *Exchange, err error) {

	if e.noExchange {
		//	name empty so routingKey equal queueName
		theQ, err := e.channel.QueueDeclare(
			e.queueName,
			e.durable,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return nil, err
		}

		e.routingKey = theQ.Name

	} else {

		err = e.channel.ExchangeDeclare(
			e.exchangeName,
			e.kind,
			e.durable,
			false,
			false,
			false,
			nil,
		)
	}

	return e, err
}

// ***************************

func (e *Exchange) PublishBCtx(ctx context.Context, body []byte) error {

	return e.channel.PublishWithContext(
		ctx,
		e.exchangeName,
		e.routingKey,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         body,
		},
	)
}

func (e *Exchange) PublishB(body []byte) error {
	return e.PublishBCtx(context.Background(), body)
}

func (e *Exchange) PublishCtx(ctx context.Context, body string) error {
	return e.PublishBCtx(ctx, []byte(body))
}

func (e *Exchange) Publish(body string) error {
	return e.PublishCtx(context.Background(), body)
}

func (e *Exchange) PublishBWithKeyCtx(ctx context.Context, key string, body []byte) error {
	return e.channel.PublishWithContext(
		ctx,
		e.exchangeName,
		key,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         body,
		},
	)
}

func (e *Exchange) PublishBWithKey(key string, body []byte) error {
	return e.PublishBWithKeyCtx(context.Background(), key, body)
}

func (e *Exchange) PublishWithKeyCtx(ctx context.Context, key, body string) error {
	return e.PublishBWithKeyCtx(ctx, key, []byte(body))
}

func (e *Exchange) PublishWithKey(key, body string) error {
	return e.PublishWithKeyCtx(context.Background(), key, body)
}

// ***************************

type Queue struct {
	channel        *amqp.Channel
	queueName      string
	durable        bool
	autoDelete     bool
	queueExclusive bool

	noWait bool
	args   amqp.Table

	useBind         bool
	exchange        *Exchange
	bindRoutingKeys []string

	useQos bool

	prefetchCount int
	global        bool

	consumerExclusive bool
	autoAck           bool
}

func (c *Client) NewQueue(name string) *Queue {
	return &Queue{
		channel:        c.channel,
		queueName:      name,
		durable:        false,
		autoDelete:     false,
		queueExclusive: false,
		noWait:         false,
		args:           nil,

		useBind:         false,
		bindRoutingKeys: make([]string, 0),

		useQos:        false,
		prefetchCount: 0,
		global:        false,

		autoAck: true,
	}
}

func (c *Client) DefQueue() *Queue {
	theQ := c.NewQueue("")
	// 无名队列默认设置为排他性队列
	theQ.queueExclusive = true
	return theQ
}

func (q *Queue) SetDurable(durable bool) *Queue {
	q.durable = durable
	return q
}

func (q *Queue) SetQueueExclusive(exclusive bool) *Queue {
	q.queueExclusive = exclusive
	return q
}

func (q *Queue) SetQos(count int, global bool) *Queue {
	q.useQos = true
	q.prefetchCount = count
	q.global = global
	return q
}

func (q *Queue) SetQosCount(count int) *Queue {
	return q.SetQos(count, false)
}

func (q *Queue) SetAutoAck(ack bool) *Queue {
	q.autoAck = ack
	return q
}

func (q *Queue) BindExchange(exchange *Exchange) *Queue {
	q.useBind = true
	q.exchange = exchange
	return q
}

// SetBindKeys 如何设置则自定义队列绑定key，否则使用exchange中定义的key
func (q *Queue) SetBindKeys(keys []string) *Queue {
	if len(keys) > 0 {
		q.bindRoutingKeys = keys
	} else {
		q.bindRoutingKeys = append(q.bindRoutingKeys, q.exchange.routingKey)
	}
	return q
}

func (q *Queue) Do() (queue *Queue, err error) {

	theQ, err := q.channel.QueueDeclare(
		q.queueName,
		q.durable,
		false,
		q.queueExclusive,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// important：Redefine the queue name
	q.queueName = theQ.Name

	if q.useQos {
		err = q.channel.Qos(q.prefetchCount, 0, q.global)
		if err != nil {
			return nil, err
		}
	}

	if q.useBind {

		for _, key := range q.bindRoutingKeys {
			err = q.channel.QueueBind(
				theQ.Name,
				key,
				q.exchange.exchangeName,
				false,
				nil,
			)
			if err != nil {
				return nil, err
			}
		}
	}

	return q, nil
}

func (q *Queue) BaseConsume(handler func(d amqp.Delivery)) error {
	deliveries, err := q.channel.Consume(
		q.queueName,
		"",        // consumer 消费者标识
		q.autoAck, // autoAck 是否自动应答
		false,     // exclusive 是否独占
		false,     // noLocal
		false,     // noWait 是否阻塞
		nil,       // args
	)
	if err != nil {
		return err
	}

	go func() {
		for d := range deliveries {
			handler(d)
		}
	}()

	return nil
}

func (q *Queue) Consume(handler func(m *XMessage)) error {
	deliveries, err := q.channel.Consume(
		q.queueName,
		"",        // consumer 消费者标识
		q.autoAck, // autoAck 是否自动应答
		false,     // exclusive 是否独占
		false,     // noLocal
		false,     // noWait 是否阻塞
		nil,       // args
	)
	if err != nil {
		return err
	}

	go func() {
		for d := range deliveries {
			msg := &XMessage{d}
			handler(msg)
		}
	}()

	return nil
}
