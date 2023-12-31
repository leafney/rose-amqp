/**
 * @Author:      leafney
 * @GitHub:      https://github.com/leafney
 * @Project:     rose-amqp
 * @Date:        2023-12-17 16:29
 * @Description:
 */

package ramqp

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"testing"
	"time"
)

const (
	AMQPUrl = "amqp://test:123@192.168.8.105:5672/"
)

func TestOne(t *testing.T) {
	// 第一版测试

	/*
		client, err := NewClient(AMQPUrl)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		//	发布消息
		go func() {
			i := 0
			for {
				if !client.IsConnected() {
					log.Println("sleep wait connect")
					time.Sleep(1 * time.Second)
					continue
				}

				msg := fmt.Sprintf("hello %d", i)
				log.Printf("publish %v", msg)
				err := client.Publish(context.Background(), "test", msg)
				if err != nil {
					t.Error(err)
				}

				log.Println("sleep sleep")
				time.Sleep(5 * time.Second)
				i += 1
			}
		}()

		log.Println("发布完毕")
		//time.Sleep(10 * time.Second)

		//go func() {
		//	err := client.Consume("test", func(d amqp.Delivery) {
		//		msg := string(d.Body)
		//		t.Logf("接收到消息 %v", msg)
		//		time.Sleep(5 * time.Second)
		//		d.Ack(false)
		//	})
		//	if err != nil {
		//		t.Error(err)
		//	}
		//
		//}()

		//time.Sleep(30 * time.Second)

		select {}

	*/
}

// direct Routing mode
func TestTwo(t *testing.T) {
	client, err := NewClient(AMQPUrl)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	//

	eee, err := client.NewExchange("hello").SetKind(KindDirect).
		Do()
	if err != nil {
		t.Error(err)
	}

	qq, err := client.NewQueue("world").
		BindExchange(eee).
		Do()
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("hello %d", i)
		log.Printf("publish %v", msg)
		if err := eee.PublishCtx(context.Background(), msg); err != nil {
			t.Error(err)
		}
	}

	go func() {
		qq.BaseConsume(func(d amqp.Delivery) {
			msg := string(d.Body)
			t.Logf("接收到消息 %v", msg)
			time.Sleep(5 * time.Second)
			d.Ack(false)
		})
	}()

	select {}
}

// work queues
func TestThree(t *testing.T) {
	client, err := NewClient(AMQPUrl)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// producer
	e1, err := client.NoExchangeOnlyQueue("aaa").Do()
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("hello %d", i)
		log.Printf("publish %v", msg)
		if err := e1.PublishCtx(context.Background(), msg); err != nil {
			t.Error(err)
		}
	}

	// consumer 1
	go func() {
		q1, err := client.NewQueue("aaa").SetQos(1, false).Do()
		if err != nil {
			t.Error(err)
			return
		}
		q1.BaseConsume(func(d amqp.Delivery) {
			msg := string(d.Body)
			t.Logf("[111] 接收到消息 %v", msg)
			time.Sleep(5 * time.Second)
			d.Ack(false)
		})

	}()

	// consumer 2
	go func() {
		q1, err := client.NewQueue("aaa").SetQos(1, false).Do()
		if err != nil {
			t.Error(err)
			return
		}
		q1.BaseConsume(func(d amqp.Delivery) {
			msg := string(d.Body)
			t.Logf("[222] 接收到消息 %v", msg)
			time.Sleep(5 * time.Second)
			d.Ack(false)
		})

	}()

	select {}
}

// fanout Publish/Subscribe
func TestFour(t *testing.T) {
	client, err := NewClient(AMQPUrl)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	//
	//client.channel.ExchangeDeclare(
	//	"ccc",
	//	amqp.ExchangeFanout,
	//	true,
	//	false,
	//	false,
	//	false,
	//	nil,
	//)

	//go func() {
	//	q, _ := client.channel.QueueDeclare(
	//		"",
	//		false,
	//		false,
	//		true,
	//		false,
	//		nil,
	//	)
	//
	//	client.channel.QueueBind(q.Name,
	//		"",
	//		"bbb",
	//		false,
	//		nil,
	//	)
	//	msgs, _ := client.channel.Consume(
	//		q.Name,
	//		"",
	//		true,
	//		false,
	//		false,
	//		false,
	//		nil,
	//	)
	//	go func() {
	//		for d := range msgs {
	//			msg := string(d.Body)
	//			t.Logf("[111] 接收到消息 %v", msg)
	//		}
	//	}()
	//}()

	//go func() {
	//	q, _ := client.channel.QueueDeclare(
	//		"",
	//		false,
	//		false,
	//		true,
	//		false,
	//		nil,
	//	)
	//
	//	client.channel.QueueBind(q.Name,
	//		"",
	//		"bbb",
	//		false,
	//		nil,
	//	)
	//	msgs, _ := client.channel.Consume(
	//		q.Name,
	//		"",
	//		true,
	//		false,
	//		false,
	//		false,
	//		nil,
	//	)
	//	go func() {
	//		for d := range msgs {
	//			msg := string(d.Body)
	//			t.Logf("[222] 接收到消息 %v", msg)
	//		}
	//	}()
	//}()

	//for i := 0; i < 10; i++ {
	//	time.Sleep(3 * time.Second)
	//
	//	msg := fmt.Sprintf("hello %d", i)
	//	log.Printf("publish %v", msg)
	//	client.channel.PublishWithContext(context.Background(),
	//		"ccc",
	//		"",
	//		false,
	//		false,
	//		amqp.Publishing{
	//			ContentType: "text/plain",
	//			Body:        []byte(msg),
	//		},
	//	)
	//}

	// producer
	e2, err := client.NewExchange("bbb").SetKind(KindFanout).SetDurable(true).Do()
	if err != nil {
		t.Error(err)
		return
	}

	// consumer 1
	go func() {
		q2, err := client.
			DefQueue().
			//SetQos(1, false).
			BindExchange(e2).
			Do()
		if err != nil {
			t.Error(err)
			return
		}
		q2.BaseConsume(func(d amqp.Delivery) {
			msg := string(d.Body)
			t.Logf("[111] 接收到消息 %v", msg)
			//time.Sleep(5 * time.Second)
			//d.Ack(false)
		})

	}()

	// consumer 2
	go func() {
		q3, err := client.DefQueue().
			//SetQos(1, false).
			BindExchange(e2).
			Do()
		if err != nil {
			t.Error(err)
			return
		}
		q3.BaseConsume(func(d amqp.Delivery) {
			msg := string(d.Body)
			t.Logf("[222] 接收到消息 %v", msg)
			//time.Sleep(5 * time.Second)
			//d.Ack(false)
		})

	}()

	for i := 0; i < 10; i++ {
		time.Sleep(3 * time.Second)

		msg := fmt.Sprintf("hello %d", i)
		log.Printf("publish %v", msg)
		if err := e2.Publish(msg); err != nil {
			t.Error(err)
		}
	}

	select {}
}

// topics
func TestFive1(t *testing.T) {
	client, err := NewClient(AMQPUrl)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	//	exchange

	e5, err := client.
		NewExchange("ddd").
		SetKind(KindTopic).
		SetDurable(true).
		//SetRoutingKey(""). // routingKey 可以在这里声明，统一的
		Do()
	if err != nil {
		t.Error(err)
		return
	}

	//	consumer 1
	q5, err := client.DefQueue().
		BindExchange(e5).
		SetBindKeys([]string{"orange.*"}).Do()
	if err != nil {
		t.Error(err)
		return
	}

	//q5.Consume(func(d amqp.Delivery) {
	//	msg := string(d.Body)
	//	t.Logf("[555] 接收到消息 %v", msg)
	//})

	q5.Consume(func(msg *XMessage) {
		msg.Success()

	})

	// publish
	e5.SetRoutingKey("orange.hello"). // routingKey 也可以在这里声明，可以设置每次发布不同值
						Publish("hello1")

	time.Sleep(5 * time.Second)

	e5.
		SetRoutingKey("apple.hello"). // routingKey 也可以在这里声明，可以设置每次发布不同值
		Publish("world1")

	log.Println("publish end")

	select {}
}

// topic
func TestFive2(t *testing.T) {
	client, err := NewClient(AMQPUrl)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	client.channel.ExchangeDeclare(
		"eee",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)

	q6, _ := client.channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)

	log.Printf("q5 queue name [%v]", q6.Name)

	client.channel.QueueBind(
		q6.Name,
		"*.com",
		"eee",
		false,
		nil,
	)

	msgs, _ := client.channel.Consume(
		q6.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	go func() {
		for d := range msgs {
			msg := string(d.Body)
			t.Logf("[555-555] 接收到消息 %v", msg)
		}
	}()

	log.Println("publish start")

	msg1 := "hello"
	key1 := "kook.com"
	log.Printf("publish msg1 %v with key1 %v", msg1, key1)
	client.channel.PublishWithContext(context.Background(),
		"eee",
		key1,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(msg1),
		},
	)

	log.Println("publish end")
	select {}
}
