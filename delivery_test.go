package rabbitgo

import (
    "testing"
    . "github.com/smartystreets/goconvey/convey"
    //"github.com/streadway/amqp"
    "github.com/golang/protobuf/proto"
    "github.com/entropyx/rabbitgo/pb"
)

func TestDelivery(t *testing.T) {
  Convey("Given a new delivery", t, func() {
    delivery := &Delivery{}

    Convey("When a proto is sent as the response", func() {
      test := &pb.Test{Text: "some text"}
      delivery.Proto(test)

      Convey("The unmarshaled body should be the same as the proto", func() {
        newTest := &pb.Test{}
        proto.Unmarshal(delivery.Response.Body, newTest)
        So(newTest.Text, ShouldEqual, test.Text)
      })
    })
  })
}
