package rabbitgo

import (
	//"errors"
	"fmt"
	"sync"
	"time"

	log "github.com/koding/logging"
	"github.com/streadway/amqp"
)

type Consumer struct {
	conn       *Connection
	e          *Exchange
	q          *Queue
	deliveries <-chan amqp.Delivery
	handler    func(*Delivery)
	handlerRPC func(*Delivery)
	cc         *ConsumerConfig
	bc         *BindingConfig
	closed     bool
	lock       sync.Mutex
	// A notifiyng channel for publishings
	// will be used for sync. between close channel and consume handler
	done chan error
}

type ConsumerConfig struct {
	Tag           string
	PrefetchCount int
	PrefetchSize  int
	MaxDeliveries int
	Timeout       int
	AutoAck       bool
	Exclusive     bool
	NoLocal       bool
	NoWait        bool
	Args          amqp.Table
}

type BindingConfig struct {
	RoutingKey string
	NoWait     bool
	Args       amqp.Table
}

func (c *Consumer) Deliveries() <-chan amqp.Delivery {
	return c.deliveries
}

// NewConsumer is a constructor for consumer creation
// Accepts Exchange, Queue, BindingOptions and ConsumerOptions
func (c *Connection) NewConsumer(e *Exchange, q *Queue, bc *BindingConfig, cc *ConsumerConfig) (*Consumer, error) {
	consumer, err := c.newConsumer(e, q, bc, cc)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func (c *Connection) newConsumer(e *Exchange, q *Queue, bc *BindingConfig, cc *ConsumerConfig) (*Consumer, error) {
	consumer := &Consumer{
		conn: c,
		done: make(chan error),
		cc:   cc,
		bc:   bc,
		e:    e,
		q:    q,
	}
	if err := consumer.connect(); err != nil {
		return nil, err
	}
	return consumer, nil
}

// connect internally declares the exchanges and queues
func (c *Consumer) connect() error {
	channel := c.conn.pickChannel()
	defer c.conn.queue.Push(channel)
	_, err := channel.QueueDeclare(
		c.q.Name,       // name of the queue
		c.q.Durable,    // durable
		c.q.AutoDelete, // delete when usused
		c.q.Exclusive,  // exclusive
		c.q.NoWait,     // noWait
		c.q.Args,       // arguments
	)
	if err != nil {
		return err
	}
	if c.cc.PrefetchCount > 0 || c.cc.PrefetchSize > 0 {
		err = channel.Qos(
			c.cc.PrefetchCount, // prefetch count
			c.cc.PrefetchSize,  // prefetch size
			false,              // global
		)
	}
	if err != nil {
		return err
	}
	if c.e != nil {
		err := channel.ExchangeDeclare(
			c.e.Name,       // name of the exchange
			c.e.Type,       // type
			c.e.Durable,    // durable
			c.e.AutoDelete, // delete when complete
			c.e.Internal,   // internal
			c.e.NoWait,     // noWait
			c.e.Args,       // arguments
		)
		if err != nil {
			return err
		}
		err = channel.QueueBind(
			// bind to real queue
			c.q.Name,        // name of the queue
			c.bc.RoutingKey, // bindingKey
			c.e.Name,        // sourceExchange
			c.bc.NoWait,     // noWait
			c.bc.Args,       // arguments
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) consume() (*amqp.Channel, error) {
	channel := c.conn.pickChannel()
	deliveries, err := channel.Consume(
		c.q.Name,       // name
		c.cc.Tag,       // consumerTag,
		c.cc.AutoAck,   // autoAck
		c.cc.Exclusive, // exclusive
		c.cc.NoLocal,   // noLocal
		c.cc.NoWait,    // noWait
		c.cc.Args,      // arguments
	)
	// should we stop streaming, in order not to consume from server?
	c.deliveries = deliveries
	if check := c.limitChannels(); check == true {
		go func() {
			for c.conn.usless.Len() > 0 {
				ch := c.conn.usless.Poll().(*amqp.Channel)
				ch.Close()
			}
		}()
	}
	return channel, err
}

// Consume accepts a handler function for every message streamed from RabbitMq
// will be called within this handler func
func (c *Consumer) Consume(handler func(delivery *Delivery)) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	count := 0
	ch, err := c.consume()
	if err != nil {
		return err
	}
	go func() {
		if timeout := c.cc.Timeout; timeout > 0 {
			time.Sleep(time.Duration(timeout) * time.Millisecond)
			if c.closed == false {
				log.Error(fmt.Sprintf("Timeout in %d ms", timeout))
				c.Cancel(ch)
			}
		}
	}()
	c.handler = handler
	log.Info("handle: deliveries channel starting")
	// handle all consumer errors, if required re-connect
	// there are problems with reconnection logic for now
	for d := range c.deliveries {
		delivery := &Delivery{&d, c, false, nil, nil, false}
		handler(delivery)
		count++
		if count >= c.cc.MaxDeliveries {
			c.Cancel(ch)
		}
	}
	c.conn.queue.Push(ch)
	log.Info("handle: deliveries channel closed")
	//This was blocking the flow. Not sure how needed is.
	//c.done <- nil
	return nil
}

// ConsumeRPC accepts a handler function for every message streamed from RabbitMq
// will be called within this handler func.
// It returns the message to send and the content type.
func (c *Consumer) ConsumeRPC(handler func(delivery *Delivery)) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, err := c.consume()
	if err != nil {
		return err
	}
	c.handlerRPC = handler

	log.Info("handle: deliveries channel starting")

	// handle all consumer errors, if required re-connect
	// there are problems with reconnection logic for now
	for d := range c.deliveries {
		go func(d *amqp.Delivery) {
			delivery := &Delivery{d, c, false, nil, nil, false}
			replyTo := delivery.ReplyTo
			handler(delivery)
			if delivery.Delegated == true {
				if err = delivery.AckError; err != nil {
					log.Error("Unable to delegate an acknowledgement: " + err.Error())
				}
				return
			}
			response := delivery.Response
			fmt.Println("delivery.Response", delivery.Response)
			fmt.Println("delivery.CorrelationId", delivery.CorrelationId)
			if response != nil {
				if replyTo == "" {
					log.Error("Response was ready, but received an empty routing key. Delivery was rejected.")
					delivery.RejectOrSkip(false)
					return
				}
				publishing := &amqp.Publishing{
					Body:          response.Body,
					CorrelationId: delivery.CorrelationId,
				}
				channel := c.conn.pickChannel()
				err := channel.Publish(
					"",
					replyTo,
					false,
					false,
					*publishing,
				)
				c.conn.queue.Push(channel)
				if err != nil {
					log.Error("Unable to reply back: " + err.Error())
				}
			}
			delivery.AckOrSkip(delivery.preAckMultiple)
		}(&d)
	}
	log.Info("handle: deliveries channel closed")
	c.done <- nil
	return nil
}

// QOS controls how many messages the server will try to keep on the network for
// consumers before receiving delivery acks.  The intent of Qos is to make sure
// the network buffers stay full between the server and client.
func (c *Consumer) QOS(messageCount int) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	channel := c.conn.pickChannel()
	c.conn.queue.Push(channel)
	return channel.Qos(messageCount, 0, false)
}

// ConsumeMessage accepts a handler function and only consumes one message
// stream from RabbitMq
func (c *Consumer) Get(handler func(delivery *Delivery)) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	channel := c.conn.pickChannel()
	m, ok, err := channel.Get(c.q.Name, c.cc.AutoAck)
	message := &Delivery{&m, c, false, nil, nil, false}
	if err != nil {
		return err
	}
	c.handler = handler
	if ok {
		fmt.Println("Message received")
		handler(message)
	} else {
		fmt.Println("No message received")
	}
	c.conn.queue.Push(channel)
	// TODO maybe we should return ok too?
	return nil
}

/*func (c *Consumer) Shutdown() error {
	co := c.cc
	if err := shutdownChannel(c.ch, co.Tag); err != nil {
		return err
	}
	fmt.Println("Waiting for Consumer handler to exit")
	// if we have not called the Consume yet, we can return here
	if c.deliveries == nil {
		close(c.done)
	}
	fmt.Printf("deliveries %s", c.deliveries)
	c.closed = true
	fmt.Println("Consumer shutdown OK")
	// this channel is here for finishing the consumer's ranges of
	// delivery chans.  We need every delivery to be processed, here make
	// sure to wait for all consumers goroutines to finish before exiting our
	// process.
	return nil
}*/

func (c *Consumer) Cancel(ch *amqp.Channel) {
	err := ch.Cancel(c.cc.Tag, c.cc.NoWait)
	fmt.Println("canceled")
	if err != nil {
		fmt.Println(err)
	}
	c.closed = true
}

func (c *Consumer) limitChannels() bool {
	c.conn.lock.Lock()
	defer c.conn.lock.Unlock()
	if c.conn.queue.Len() >= c.conn.config.MaxChannels {
		for i := c.conn.queue.Len(); i >= c.conn.config.MinChannels; i-- {
			ch := c.conn.queue.Poll().(*amqp.Channel)
			c.conn.usless.Push(ch)
		}
		return true
	}
	return false
}
