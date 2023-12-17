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

func TestNewClient(t *testing.T) {

	client, err := NewClient("amqp://test:123@192.168.8.105:5672/")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	//	发布消息
	ctx := context.Background()
	err = client.Publish(ctx, "test", "hello")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	err = client.Consume("test", func(d amqp.Delivery) {
		msg := string(d.Body)
		t.Logf("接收到消息 %v", msg)
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Second)
}
