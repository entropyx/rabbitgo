package rabbitgo

import (
	"time"
	//"errors"
	"github.com/entropyx/rabbitgo/utils"
	"github.com/streadway/amqp"
)

type Producer struct {
	conn *Connection
	e    *Exchange
	q    *Queue
	pc   *ProducerConfig
}

type ProducerConfig struct {
	Exchange string
	// The key that when publishing a message to a exchange/queue will be only delivered to
	// given routing key listeners
	RoutingKey string
	// Publishing tagpackage
	Tag string
	// Maximum waiting time in miliseconds
	Timeout int
	// Maximum number of deliveries that will be received
	MaxDeliveries int
	// Queue should be on the server/broker
	Mandatory bool
	// Consumer should be bound to server
	Immediate bool
}

// TODO: Should we use this instead of amqp.Publishing?
type Publishing struct {
	// Application or exchange specific fields,
	// the headers exchange will inspect this field.
	// TODO: convert to amqp.Table
	Headers map[string]interface{}
	// Properties
	ContentType     string    // MIME content type
	ContentEncoding string    // MIME content encoding
	DeliveryMode    uint8     // Transient (0 or 1) or Persistent (2)
	Priority        uint8     // 0 to 9
	CorrelationId   string    // correlation identifier
	ReplyTo         string    // address to to reply to (ex: RPC)
	Expiration      string    // message expiration spec
	MessageId       string    // message identifier
	Timestamp       time.Time // message timestamp
	Type            string    // message type name
	UserId          string    // creating user id - ex: "guest"
	AppId           string    // creating application id
	// The application specific payload of the message
	Body []byte
}

func (c *Connection) NewProducer(pc *ProducerConfig) (*Producer, error) {
	return &Producer{
		conn: c,
		pc:   pc,
	}, nil
}

func (p *Producer) Publish(publishing *amqp.Publishing) error {
	pc := p.pc
	routingKey := pc.RoutingKey
	channel := p.conn.pickChannel()
	defer p.conn.queue.Push(channel)
	err := channel.Publish(
		pc.Exchange,  // publish to an exchange(it can be default exchange)
		routingKey,   // routing to 0 or more queues
		pc.Mandatory, // mandatory, if no queue than err
		pc.Immediate, // immediate, if no consumer than err
		*publishing,
	)
	return err
}

// PublishRPC accepts a handler function for every message streamed from RabbitMq
// as a reply after publishing a message.
func (p *Producer) PublishRPC(publishing *amqp.Publishing, handler func(delivery *Delivery)) error {
	maxDeliveries := p.pc.MaxDeliveries
	if maxDeliveries <= 0 {
		maxDeliveries = 1
	}
	randString := utils.RandomString(35)
	queue := &Queue{
		Name:       "queue_" + randString,
		AutoDelete: true,
		Exclusive:  true,
	}
	consumerConfig := &ConsumerConfig{
		Tag:           "consumer_" + randString,
		Timeout:       p.pc.Timeout,
		MaxDeliveries: maxDeliveries,
	}
	consumer, err := p.conn.newConsumer(nil, queue, nil, consumerConfig)
	if err != nil {
		return err
	}
	publishing.CorrelationId = randString
	publishing.ReplyTo = queue.Name
	err = p.Publish(publishing)
	if err != nil {
		return err
	}
	err = consumer.Consume(func(d *Delivery) {
		if randString == d.CorrelationId {
			handler(d)
			d.Ack(true)
		}
	})
	return err
}

// NotifyReturn captures a message when a Publishing is unable to be
// delivered either due to the `mandatory` flag set
// and no route found, or `immediate` flag set and no free consumer.
func (p *Producer) NotifyReturn(notifier func(message amqp.Return)) {
	go func() {
		channel := p.conn.pickChannel()
		for res := range channel.NotifyReturn(make(chan amqp.Return)) {
			notifier(res)
		}
		p.conn.queue.Push(channel)
	}()
}

func (p *Producer) NotifyPublish(confirmer func(message amqp.Confirmation)) {
	go func() {
		channel := p.conn.pickChannel()
		for res := range channel.NotifyPublish(make(chan amqp.Confirmation)) {
			confirmer(res)
		}
		p.conn.queue.Push(channel)
	}()
}
