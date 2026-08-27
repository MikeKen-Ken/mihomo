package adapter

import "testing"

func TestDelayReachedTimeout(t *testing.T) {
	if delayReachedTimeout(299, 300) {
		t.Fatal("delay below timeout must remain successful")
	}
	if !delayReachedTimeout(300, 300) {
		t.Fatal("delay equal to timeout must fail")
	}
	if !delayReachedTimeout(301, 300) {
		t.Fatal("delay above timeout must fail")
	}
}
