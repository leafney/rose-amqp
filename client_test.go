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
	amqp "github.com/rabbitmq/amqp091-go"
	"testing"
	"time"
)

const (
	AMQPUrl = "amqp://test:123@192.168.8.105:5672/"
)

func TestNewClient(t *testing.T) {

	client, err := NewClient(AMQPUrl)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	//	发布消息
	ctx := context.Background()
	err = client.Publish(ctx, "test", "hello")
	if err != nil {
		//t.Fatal(err)
		t.Error(err)
	}

	time.Sleep(5 * time.Second)

	err = client.Consume("test", func(d amqp.Delivery) {
		msg := string(d.Body)
		t.Logf("接收到消息 %v", msg)
	})
	if err != nil {
		//t.Fatal(err)
		t.Error(err)
	}

	//time.Sleep(30 * time.Second)

	select {}
}
