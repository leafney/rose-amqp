/**
 * @Author:      leafney
 * @GitHub:      https://github.com/leafney
 * @Project:     rose-amqp
 * @Date:        2023-12-24 10:32
 * @Description:
 */

package ramqp

import amqp "github.com/rabbitmq/amqp091-go"

type Exchange struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

func NewQueue(name string) *Queue {
	return &Queue{
		Name:       name,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
	}
}

func DefaultQueue() *Queue {
	return &Queue{
		Name:       "",
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
	}
}

func (q *Queue) SetDurable(durable bool) *Queue {
	q.Durable = durable
	return q
}
