package provider

import (
	"context"
	"testing"
)

func TestClampHealthCheckWorkerLimit(t *testing.T) {
	tests := []struct {
		input int
		want  int
	}{
		{input: -1, want: 30},
		{input: 0, want: 30},
		{input: 5, want: 5},
		{input: 30, want: 30},
		{input: 31, want: 30},
		{input: 200, want: 30},
	}
	for _, test := range tests {
		if got := ClampHealthCheckWorkerLimit(test.input); got != test.want {
			t.Fatalf("ClampHealthCheckWorkerLimit(%d) = %d, want %d", test.input, got, test.want)
		}
	}
}

func TestHealthCheckWorkerCeilingIsShared(t *testing.T) {
	SetHealthCheckWorkerLimit(5)
	defer SetHealthCheckWorkerLimit(30)

	for i := 0; i < 5; i++ {
		if !AcquireHealthCheckWorker(context.Background()) {
			t.Fatal("failed to fill worker slots")
		}
	}
	defer func() {
		for i := 0; i < 5; i++ {
			ReleaseHealthCheckWorker()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if AcquireHealthCheckWorker(ctx) {
		ReleaseHealthCheckWorker()
		t.Fatal("acquired a worker after the shared ceiling was full")
	}
}
