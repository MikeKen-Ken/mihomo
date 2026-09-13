package sing_vless

import (
	"encoding/base64"
	"testing"

	LC "github.com/metacubex/mihomo/listener/config"
)

func TestConstructorErrorAfterDecryptionDoesNotPanic(t *testing.T) {
	key := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	listener, err := New(LC.VlessServer{
		Decryption:  "mlkem768x25519plus.native.0s." + key,
		Certificate: "invalid certificate", PrivateKey: "invalid key",
	}, nil)
	if err == nil || listener != nil {
		t.Fatalf("expected constructor failure, got %v, %v", listener, err)
	}
}
