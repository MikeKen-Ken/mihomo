package adapter

import (
	"context"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDiagnosticFailureDoesNotPoisonNodeHealth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	p := NewProxy(&delayedDialAdapter{Base: outbound.NewBase(outbound.BaseOption{Name: "probe", Type: C.Direct})})
	p.RecordProbeHealth(server.URL, 20, true)
	status, _ := utils.NewUnsignedRanges[uint16]("204")
	ctx, cancel := context.WithTimeout(C.WithConnectivityProbe(context.Background()), time.Second)
	defer cancel()
	if _, err := p.URLTest(ctx, server.URL, status); err == nil {
		t.Fatal("unexpected HTTP status accepted as successful probe")
	}
	if !p.AliveForTestUrl(server.URL) || len(p.DelayHistoryForTestUrl(server.URL)) != 1 || len(p.DelayHistory()) != 0 {
		t.Fatal("diagnostic destination failure changed published node health/history")
	}
	p.RecordProbeHealth(server.URL, 0, false)
	if p.AliveForTestUrl(server.URL) {
		t.Fatal("confirmed node failure not published")
	}
}

func TestCanceledBackupProbePreservesPreviousHealth(t *testing.T) {
	p := NewProxy(&delayedDialAdapter{Base: outbound.NewBase(outbound.BaseOption{Name: "canceled", Type: C.Direct}), dialDelay: time.Second})
	url := "http://127.0.0.1:1"
	p.RecordProbeHealth(url, 20, true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.URLTest(ctx, url, nil)
	if !p.AliveForTestUrl(url) || len(p.DelayHistoryForTestUrl(url)) != 1 {
		t.Fatal("canceled probe marked backup unavailable")
	}
}
