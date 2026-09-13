package core

import (
	"reflect"
	"testing"
)

func TestUDPMessageWireRoundTrip(t *testing.T) {
	want := udpMessage{SessionID: 0x01020304, Host: "example.com", Port: 53, MsgID: 7, FragCount: 1, Data: []byte("payload")}
	wire := want.Pack()
	if len(wire) != want.Size() {
		t.Fatalf("wire size %d, want %d", len(wire), want.Size())
	}
	var got udpMessage
	if err := got.Unpack(wire); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}
