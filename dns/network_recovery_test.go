package dns

import "testing"

func TestRequestNetworkRecovery(t *testing.T) {
	SetNetworkRecoveryFunc(nil)
	if requestNetworkRecovery("missing") {
		t.Fatal("request reported success without a callback")
	}

	calledWith := ""
	SetNetworkRecoveryFunc(func(reason string) {
		calledWith = reason
	})
	t.Cleanup(func() { SetNetworkRecoveryFunc(nil) })

	if !requestNetworkRecovery("dns failed") {
		t.Fatal("request did not invoke the installed callback")
	}
	if calledWith != "dns failed" {
		t.Fatalf("callback reason = %q, want %q", calledWith, "dns failed")
	}
}
