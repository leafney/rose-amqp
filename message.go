/**
 * @Author:      leafney
 * @GitHub:      https://github.com/leafney
 * @Project:     rose-amqp
 * @Date:        2023-12-31 11:26
 * @Description:
 */

package ramqp

import "github.com/rabbitmq/amqp091-go"

type XMessage struct {
	amqp091.Delivery
}

func (m *XMessage) Success() error {
	return m.Ack(false)
}

func (m *XMessage) FailRetry() error {
	return m.Nack(false, true)
}

func (m *XMessage) FailOut() error {
	return m.Nack(false, false)
}

func (m *XMessage) ToString() string {
	return string(m.Body)
}

func (m *XMessage) ToByte() []byte {
	return m.Body
}
