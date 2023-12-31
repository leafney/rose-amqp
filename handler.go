/**
 * @Author:      leafney
 * @GitHub:      https://github.com/leafney
 * @Project:     rose-amqp
 * @Date:        2023-12-31 11:25
 * @Description:
 */

package ramqp

import amqp "github.com/rabbitmq/amqp091-go"

type XHandler struct {
	f func(msg *XMessage)
}

func (q *XHandler) HandleMessage(delivery amqp.Delivery) {
	q.f(&XMessage{delivery})
}
