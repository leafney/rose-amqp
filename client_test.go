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

func TestTwo(t *testing.T) {
	client, err := NewClient(AMQPUrl)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	//

	if err := client.NewExchange("hello").SetType("direct").
		Do(); err != nil {
		t.Error(err)
	}

	qq, err := client.NewQueue("world").
		BindExchange("hello", "nihao").
		Do()
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("hello %d", i)
		log.Printf("publish %v", msg)
		if err := qq.Publish(context.Background(), msg); err != nil {
			t.Error(err)
		}
	}

	go func() {
		qq.Consume(func(d amqp.Delivery) {
			msg := string(d.Body)
			t.Logf("接收到消息 %v", msg)
			time.Sleep(5 * time.Second)
			d.Ack(false)
		})
	}()

	select {}
}
