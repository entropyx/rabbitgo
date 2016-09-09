package rabbitgo

import (
  //"fmt"
  "github.com/streadway/amqp"
  //log "github.com/koding/logging"
)

// Delivery captures the fields for a previously delivered message resident in a
// queue to be delivered by the server to a consumer from Consumer.Consume or
// Consumer.Get.
type Delivery struct {
  *amqp.Delivery
  consumer *Consumer
  Delegated bool
  AckError error
}

/*
  Delegate delegates an acknowledgement through the amqp.Acknowledger interface.
  It must be called during a handler execution.

  Either [ack](https://godoc.org/github.com/streadway/amqp#Delivery.Ack),
  [reject](https://godoc.org/github.com/streadway/amqp#Delivery.Reject) or
  [nack](https://godoc.org/github.com/streadway/amqp#Delivery.Nack) can be used
  as the acknowledger.

  The order of the options must be exactly the same as it is required in the
  respective [amqp.Delivery](https://godoc.org/github.com/streadway/amqp#Delivery)
  function.

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
    err = d.Reject(requeue)
  default:
    panic("unknown acknowledgement")
  }
  d.Delegated = true
  d.AckError = err
  return nil
}
