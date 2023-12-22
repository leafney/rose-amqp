/**
 * @Author:      leafney
 * @GitHub:      https://github.com/leafney
 * @Project:     rose-amqp
 * @Date:        2023-12-17 15:30
 * @Description:
 */

package ramqp

import (
	"context"
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

type Client struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func connect(amqpUrl string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(amqpUrl)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func ReConnect(url string) *amqp.Connection {
	retryInterval := InitialRetryInterval
	retryDeadline := time.Now().Add(MaxRetryDuration)

	for {
		conn, err := connect(url)
		if err == nil {
			log.Println("Connected to RabbitMQ")
			return conn
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

// NewClient 创建一个新的 AMQP 客户端
func NewClient(amqpUrl string) (*Client, error) {
	//conn, err := amqp.Dial(amqpUrl)
	//if err != nil {
	//	return nil, err
	//}

	// 创建连接
	conn := ReConnect(amqpUrl)

	// 当连接断开时，自动重新连接
	go func() {
		for {
			closeErr := <-conn.NotifyClose(make(chan *amqp.Error))
			log.Printf("Connection closed error [%v] Reconnecting...", closeErr)
			conn = ReConnect(amqpUrl)
		}
	}()

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return &Client{conn, ch}, nil
}

// Close 关闭 AMQP 连接和通道
func (c *Client) Close() error {
	if err := c.channel.Close(); err != nil {
		return fmt.Errorf("channel.Close error: %v", err)
	}
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("conn.Close error: %v", err)
	}
	return nil
}

func (c *Client) Publish(ctx context.Context, queueName string, body string) error {

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
		ContentType: "text/plain",
		Body:        []byte(body),
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

	msgs, err := c.channel.Consume(
		q.Name,
		"",    // consumer 消费者标识
		true,  // autoAck 是否自动应答
		false, // exclusive 是否独占
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
