package rabbitgo

import (
  "time"
  "errors"
  "github.com/streadway/amqp"
)

type Producer struct {
  ch  *amqp.Channel
  e   *Exchange
  q   *Queue
  pc  *ProducerConfig
}

type ProducerConfig struct {
  // The key that when publishing a message to a exchange/queue will be only delivered to
	// given routing key listeners
	RoutingKey string
	// Publishing tag
	Tag string
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
   ContentType      string    // MIME content type
   ContentEncoding  string    // MIME content encoding
   DeliveryMode     uint8     // Transient (0 or 1) or Persistent (2)
   Priority         uint8     // 0 to 9
   CorrelationId    string    // correlation identifier
   ReplyTo          string    // address to to reply to (ex: RPC)
   Expiration       string    // message expiration spec
   MessageId        string    // message identifier
   Timestamp        time.Time // message timestamp
   Type             string    // message type name
   UserId           string    // creating user id - ex: "guest"
   AppId            string    // creating application id
   // The application specific payload of the message
   Body []byte
}

func (c *Connection) NewProducer(e *Exchange, q *Queue, pc *ProducerConfig) (*Producer, error) {
  if c.ch == nil {
    return nil, errors.New("No channel found. Are you using NewConnection?")
  }
  return &Producer{
    ch: c.ch,
    e:  e,
    q:  q,
    pc: pc,
  }, nil
}

func (p *Producer) Publish(publishing *amqp.Publishing) error {
  routingKey := p.pc.RoutingKey
	// if exchange name is empty, this means we are gonna publish
	// this mesage to a queue, every queue has a binding to default exchange
	if p.e.Name == "" {
		routingKey = p.q.Name
	}
  err := p.ch.Publish(
		p.e.Name,       // publish to an exchange(it can be default exchange)
		routingKey,   // routing to 0 or more queues
		p.pc.Mandatory, // mandatory, if no queue than err
		p.pc.Immediate, // immediate, if no consumer than err
		*publishing,
	)
  return err  
}
