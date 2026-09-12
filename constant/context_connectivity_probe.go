package constant

import "context"

type connectivityProbeKey struct{}

// WithConnectivityProbe marks a diagnostic request that must not publish node
// health or delay history. Failure of its destination is not a node verdict.
func WithConnectivityProbe(ctx context.Context) context.Context {
	return context.WithValue(WithSuppressGroupOutboundFailureStats(ctx), connectivityProbeKey{}, true)
}

func IsConnectivityProbe(ctx context.Context) bool {
	v, _ := ctx.Value(connectivityProbeKey{}).(bool)
	return v
}
