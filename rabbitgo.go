package rabbitgo

import (
	"errors"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Config struct {
	Host     string
	Port     int
	Username string
	Password string
	Vhost    string
}

type Connection struct {
	conn   *amqp.Connection
	ch     *amqp.Channel //TODO: Should we use the same channel?
	config *Config
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

func init() {
	formatter := &log.TextFormatter{
		FullTimestamp: true,
		ForceColors:   true,
	}
	log.SetFormatter(formatter)
}

func NewConnection(c *Config) (*Connection, error) {
	conn := &Connection{config: c}
	if err := conn.Dial(); err != nil {
		return nil, err
	}
	ch, err := conn.conn.Channel()
	if err != nil {
		return nil, err
	}
	conn.ch = ch
	return conn, nil
}

//ExchangeDeclare declares an exchange on the server.
func (c *Connection) ExchangeDeclare(exchange *Exchange) error {
	return c.ch.ExchangeDeclare(exchange.Name, exchange.Type, exchange.Durable, exchange.AutoDelete, exchange.Internal, exchange.NoWait, exchange.Args)
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
	return nil
}

func (c *Connection) Close() {
	c.conn.Close()
	c.ch.Close()
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

	if err := channel.Close(); err != nil {
		fmt.Printf("err %s", err)
		return err
	}

	return nil
}
