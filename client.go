/**
 * @Author:      leafney
 * @GitHub:      https://github.com/leafney
 * @Project:     rose-amqp
 * @Date:        2023-12-17 15:30
 * @Description:
 */

package ramqp

import (
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

const (
	InitialRetryInterval = 5 * time.Second
	MaxRetryInterval     = 600 * time.Second
	MaxRetryDuration     = 10 * time.Minute
	WaitAfterMaxDuration = 30 * time.Minute
)

var (
	errAlreadyClosed = errors.New("already closed: not connected to the AMQP server")
)

type Client struct {
	url                string
	conn               *amqp.Connection
	channel            *amqp.Channel
	connNotifyClose    chan *amqp.Error
	channelNotifyClose chan *amqp.Error
	disConnection      chan bool
	quit               chan struct{}
	isConnected        bool

	//useExchange  bool
	//exchangeName string
	//routingKey   string
}

// NewClient 创建一个新的 AMQP 客户端
func NewClient(amqpUrl string) (*Client, error) {
	client := &Client{
		url:                amqpUrl,
		connNotifyClose:    make(chan *amqp.Error),
		channelNotifyClose: make(chan *amqp.Error),
		disConnection:      make(chan bool),
		quit:               make(chan struct{}),
		isConnected:        false,
	}

	if err := client.connect(amqpUrl); err != nil {
		return nil, err
	}

	return client, nil
}

func (c *Client) connect(amqpUrl string) error {
	var err error

	c.conn, err = amqp.Dial(amqpUrl)
	if err != nil {
		return err
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		return err
	}

	// TODO 第三版
	//c.channel, err = c.DefChannel()

	c.conn.NotifyClose(c.connNotifyClose)
	c.channel.NotifyClose(c.channelNotifyClose)

	c.isConnected = true
	return nil
}

// Close 关闭 AMQP 连接和通道
func (c *Client) Close() error {
	log.Println("close")

	if !c.isConnected {
		return errAlreadyClosed
	}

	if err := c.channel.Close(); err != nil {
		return fmt.Errorf("channel.Close error: %v", err)
	}
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("conn.Close error: %v", err)
	}

	close(c.quit)
	c.isConnected = false

	return nil
}

/*
func (c *Client) changeConnect(connection *amqp.Connection, channel *amqp.Channel) {
	c.conn = connection
	c.connNotifyClose = make(chan *amqp.Error)
	c.conn.NotifyClose(c.connNotifyClose)

	c.channel = channel
	c.channelNotifyClose = make(chan *amqp.Error)
	c.channel.NotifyClose(c.channelNotifyClose)
}

func (c *Client) Listen() {
	go c.handleReconnect()
}

func (c *Client) handleReconnect() {
	// 当连接断开时，自动重新连接
	for {
		if !c.isConnected {
			// 未连接
			log.Println("Attempting to connect")
			c.reConnect()
		}

		log.Println("handleReconnect for connected")

		select {
		case <-c.quit:
			log.Println("quit")
			return
		case err := <-c.connNotifyClose:
			log.Printf("connection close notify: %v", err)
			c.isConnected = false
		case err := <-c.channelNotifyClose:
			log.Printf("channel close notify: %v", err)
			c.isConnected = false

		}
	}
}

func (c *Client) reConnect() {
	retryInterval := InitialRetryInterval
	retryDeadline := time.Now().Add(MaxRetryDuration)

	for {

		if err := c.connect(c.url); err == nil {
			log.Println("Connected to RabbitMQ")
			return
		}

		if time.Now().After(retryDeadline) {
			log.Println("Failed to reconnect after 10 minutes. Waiting for 30 minutes before retrying...")
			time.Sleep(WaitAfterMaxDuration)
			retryInterval = InitialRetryInterval
			retryDeadline = time.Now().Add(MaxRetryDuration)
		} else {

			log.Printf("Failed to reconnect. Retrying in %s ...", FormatDuration(retryInterval))
			time.Sleep(retryInterval)
			retryInterval *= 2
			if retryInterval > MaxRetryInterval {
				retryInterval = MaxRetryInterval
			}
		}
	}
}


func (c *Client) IsConnected() bool {
	return c.isConnected
}

func (c *Client) Publish(ctx context.Context, queueName string, body string) error {

	log.Printf("publish channel %v", c.channel)

	q, err := c.channel.QueueDeclare(
		queueName, // 队列名称
		true,      // 是否持久化
		false,     // 是否独占
		false,     // 是否自动删除
		false,     // 是否等待
		nil,       // 其他参数
	)
	if err != nil {
		return fmt.Errorf("channel.QueueDeclare error: %v", err)
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         []byte(body),
	}

	if err := c.channel.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		msg,
	); err != nil {
		return fmt.Errorf("channel.Publish error: %v", err)
	}
	return nil
}

func (c *Client) Consume(queueName string, handler func(delivery amqp.Delivery)) error {

	log.Printf("Consume channel %v", c.channel)

	q, err := c.channel.QueueDeclare(
		queueName, // 队列名称
		true,      // 是否持久化
		false,     // 是否独占
		false,     // 是否自动删除
		false,     // 是否等待
		nil,       // 其他参数
	)
	if err != nil {
		return fmt.Errorf("channel.QueueDeclare error: %v", err)
	}

	if err := c.channel.Qos(1, 0, true); err != nil {
		log.Println(err)
	}

	msgs, err := c.channel.Consume(
		q.Name,
		"",    // consumer 消费者标识
		false, // autoAck 是否自动应答
		false, // queueExclusive 是否独占
		false, // noLocal
		false, // noWait 是否阻塞
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("channel.Consume error: %v", err)
	}

	go func() {
		for msg := range msgs {
			handler(msg)
		}
	}()

	return nil
}

*/
