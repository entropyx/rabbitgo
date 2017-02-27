package rabbitgo

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hishboy/gocommons/lang"
	log "github.com/koding/logging"
	"github.com/streadway/amqp"
)

type Config struct {
	Host        string
	Port        int
	Username    string
	Password    string
	Vhost       string
	MinChannels int
	MaxChannels int
}

type Connection struct {
	conn   *amqp.Connection
	ch     *amqp.Channel
	config *Config
	ready  []bool
	index  int
	queue  *lang.Queue
	usless *lang.Queue
	lock   sync.Mutex
	last   *time.Time
}

type Exchange struct {
	// Exchange name
	Name string
	// Exchange type
	Type string
	// Durable exchanges will survive server restarts
	Durable bool
	// Will remain declared when there are no remaining bindings.
	AutoDelete bool
	// Exchanges declared as `internal` do not accept accept publishings.Internal
	// exchanges are useful for when you wish to implement inter-exchange topologies
	// that should not be exposed to users of the broker.
	Internal bool
	// When noWait is true, declare without waiting for a confirmation from the server.
	NoWait bool
	// amqp.Table of arguments that are specific to the server's implementation of
	// the exchange can be sent for exchange types that require extra parameters.
	Args amqp.Table
}

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

func NewConnection(c *Config) (*Connection, error) {
	conn := &Connection{config: c}
	if conn.config.MinChannels <= 0 {
		conn.config.MinChannels = 5000
	}

	if conn.config.MaxChannels <= 0 {
		conn.config.MaxChannels = 10000
	}

	conn.queue =
		lang.NewQueue()
	conn.usless = lang.NewQueue()

	if err := conn.Dial(); err != nil {
		return nil, err
	}

	ch, err := conn.conn.Channel()
	if err != nil {
		return nil, err
	}
	conn.index = 0
	conn.ch = ch
	return conn, nil
}

func (c *Connection) Publish(exchange, key string, mandatory, immediate bool, msg *amqp.Publishing) error {
	return c.ch.Publish(exchange, key, mandatory, immediate, *msg)
}

func (c *Connection) Dial() error {
	var err error
	if c.config == nil {
		return errors.New("config is nil")
	}
	uri := amqp.URI{
		Scheme:   "amqp",
		Host:     c.config.Host,
		Port:     c.config.Port,
		Username: c.config.Username,
		Password: c.config.Password,
		Vhost:    c.config.Vhost,
	}.String()
	c.conn, err = amqp.Dial(uri)
	if err != nil {
		return err
	}
	c.handleErrors(c.conn)
	for i := 0; i < c.config.MinChannels; i++ {
		go func() {
			ch, _ := c.conn.Channel()
			c.queue.Push(ch)
		}()
	}
	return nil
}

func (c *Connection) Close() {
	c.conn.Close()
	c.ch.Close()
	fmt.Println("CONEXION WAS CLOSED")
}

func (c *Connection) handleErrors(conn *amqp.Connection) {
	go func() {
		for amqpErr := range conn.NotifyClose(make(chan *amqp.Error)) {
			// if the computer sleeps then wakes longer than a heartbeat interval,
			// the connection will be closed by the client.
			// https://github.com/streadway/amqp/issues/82
			log.Fatal(amqpErr.Error())
			if strings.Contains(amqpErr.Error(), "NOT_FOUND") {
				// do not continue
			}
			// CRITICAL Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
			// CRITICAL Exception (501) Reason: "read tcp 127.0.0.1:5672: i/o timeout"
			// CRITICAL Exception (503) Reason: "COMMAND_INVALID - unimplemented method"
			if amqpErr.Code == 501 {
				// reconnect
			}
			if amqpErr.Code == 320 {
				// fmt.Println("tryin to reconnect")
				// c.reconnect()
			}
		}
	}()

	go func() {
		for b := range conn.NotifyBlocked(make(chan amqp.Blocking)) {
			if b.Active {
				log.Info("TCP blocked: %q", b.Reason)
			} else {
				log.Info("TCP unblocked")
			}
		}
	}()
}

// shutdownChannel is a general closer function for channels
func shutdownChannel(channel *amqp.Channel, tag string) error {
	// This waits for a server acknowledgment which means the sockets will have
	// flushed all outbound publishings prior to returning.  It's important to
	// block on Close to not lose any publishings.
	if err := channel.Cancel(tag, true); err != nil {
		if amqpError, isAmqpError := err.(*amqp.Error); isAmqpError && amqpError.Code != 504 {
			return fmt.Errorf("AMQP connection close error: %s", err)
		}
	}

	if err := channel.Cancel(tag, true); err != nil {
		fmt.Printf("err %s", err)
		return err
	}

	fmt.Println("shutdown")

	return nil
}

func (c *Connection) pickChannel() *amqp.Channel {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.queue.Len() == 0 {
		newChann, _ := c.conn.Channel()
		return newChann
	}
	inf := c.queue.Poll()
	if inf != nil {
		return inf.(*amqp.Channel)
	}
	newChann, _ := c.conn.Channel()
	return newChann
}
func (c *Connection) PickChannel() *amqp.Channel {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.queue.Len() == 0 {
		newChann, _ := c.conn.Channel()
		return newChann
	}
	inf := c.queue.Poll()
	if inf != nil {
		return inf.(*amqp.Channel)
	}
	newChann, _ := c.conn.Channel()
	return newChann
}
