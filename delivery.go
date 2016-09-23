package rabbitgo

import (
  //"fmt"
  "github.com/streadway/amqp"
  "github.com/golang/protobuf/proto"
  log "github.com/koding/logging"
)

const (
  protobufContentType = "application/x-protobuf"
)

// Delivery captures the fields for a previously delivered message resident in a
// queue to be delivered by the server to a consumer from Consumer.Consume or
// Consumer.Get.
type Delivery struct {
  *amqp.Delivery
  consumer *Consumer
  Delegated bool
  AckError error
  Response *response
  preAckMultiple bool
}

type response amqp.Publishing


// AckOrSkip calls Delivery.Ack if auto ack is not activated.
func (d *Delivery) AckOrSkip(multiple bool) error {
  var err error
  if d.IsAutoAck() == false {
    err = d.Ack(multiple)
  }
  return err
}

// Data writes some data into the response body
func (d *Delivery) Data(data []byte, contentType string)  {
  response := d.getResponse()
  response.Body = data
  response.ContentType = contentType
}

// Proto takes the protocol buffer and encodes it into bytes, then writes it
// into the response body through Delivery.Data
func (d *Delivery) Proto(message interface{})  {
  out, err := proto.Marshal(message.(proto.Message))
  if err != nil {
    log.Error("An error with the proto ocurred:", err)
  }
  d.Data(out, protobufContentType)
}

func (d *Delivery) getResponse() *response {
  if d.Response == nil {
    d.Response = &response{}
  }
  return d.Response
}

/*
Delegate delegates an acknowledgement through the amqp.Acknowledger interface.
It must be called during a handler execution.

Either ack, reject or nack can be used as the acknowledger.

The order of the options must be exactly the same as it is required in the
respective amqp.Delivery function.
*/
func (d *Delivery) Delegate(ack string, options ...bool) *amqp.Publishing {
  var err error
  var multiple bool
  var requeue bool

  switch ack {
  case "nack":
    multiple, requeue = options[0], options[1]
    err = d.Nack(multiple, requeue)
  case "reject":
    requeue = options[0]
    err = d.Reject(requeue)
  case "ack":
    multiple = options[0]
    err = d.Ack(multiple)
  default:
    panic("unknown acknowledgement")
  }
  d.Delegated = true
  d.AckError = err
  return nil
}

func (d *Delivery) IsAutoAck() bool {
  return d.consumer.cc.AutoAck
}

func (d *Delivery) PreAck(multiple bool) {
  d.preAckMultiple = multiple
}

// RejectOrSkip calls Delivery.Reject if auto ack is not activated.
func (d *Delivery) RejectOrSkip(requeue bool) error {
  var err error
  if d.IsAutoAck() == false {
    err = d.Reject(requeue)
  }
  return err
}
