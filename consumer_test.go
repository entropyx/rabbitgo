package rabbitgo

import (
	"os"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/streadway/amqp"
	//"github.com/streadway/amqp"
)

func TestConsumer(t *testing.T) {
	Convey("Given a new delivery", t, func() {
		rabbitPort, _ := strconv.Atoi(os.Getenv("AMQP_PORT"))
		config := &Config{
			Host:     os.Getenv("AMQP_HOST"),
			Username: os.Getenv("AMQP_USER"),
			Password: os.Getenv("AMQP_PASS"),
			Vhost:    os.Getenv("AMQP_VHOST"),
			Port:     rabbitPort,
		}
		conn, err := NewConnection(config)
		if err != nil {
			panic(err)
		}
		go func() {
			createConsumer(conn, "test", func(d *Delivery) {
				d.Data([]byte("pong!"), "contentType")
			})
			forever := make(chan bool)
			<-forever

		}()
		pc := &ProducerConfig{
			Exchange:   "test",
			RoutingKey: "test",
			Timeout:    5 * time.Minute,
		}
		producer, _ := conn.NewProducer(pc)
		publishing := &amqp.Publishing{
			Body: []byte("ping!"),
		}
		Convey("PublishRPC", func() {
			var body []byte
			for i := 0; i < 10; i++ {
				producer.PublishRPC(publishing, func(delivery *Delivery) {
					body = delivery.Body
				})
				So(body, ShouldNotBeEmpty)
			}
		})
	})
}

func createConsumer(conn *Connection, name string, handler func(*Delivery)) {
	exchange := &Exchange{
		Name:    "test",
		Type:    "topic",
		Durable: true,
	}

	queue := &Queue{
		Name: "test",
	}
	binding := &BindingConfig{
		RoutingKey: "test",
	}
	consumerConfig := &ConsumerConfig{
		Tag:           "test",
		PrefetchCount: 1,
		AutoAck:       true,
	}
	consumer, err := conn.NewConsumer(exchange, queue, binding, consumerConfig)
	if err != nil {
		panic(err)
	}
	err = consumer.ConsumeRPC(handler)
}
