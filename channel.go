package rabbitgo

import "github.com/streadway/amqp"

type Channel struct {
	*amqp.Channel
	id int
}
