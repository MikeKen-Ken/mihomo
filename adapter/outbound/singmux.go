package outbound

import (
	"context"
	"errors"
	"sync"

	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/proxydialer"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	mux "github.com/metacubex/sing-mux"
	E "github.com/metacubex/sing/common/exceptions"
	M "github.com/metacubex/sing/common/metadata"
)

type SingMux struct {
	ProxyAdapter
	clientMu      sync.RWMutex
	client        *mux.Client
	clientOptions mux.Options
	closed        bool
	onlyTcp       bool
}

type SingMuxOption struct {
	Enabled        bool         `proxy:"enabled,omitempty"`
	Protocol       string       `proxy:"protocol,omitempty"`
	MaxConnections int          `proxy:"max-connections,omitempty"`
	MinStreams     int          `proxy:"min-streams,omitempty"`
	MaxStreams     int          `proxy:"max-streams,omitempty"`
	Padding        bool         `proxy:"padding,omitempty"`
	Statistic      bool         `proxy:"statistic,omitempty"`
	OnlyTcp        bool         `proxy:"only-tcp,omitempty"`
	BrutalOpts     BrutalOption `proxy:"brutal-opts,omitempty"`
}

type BrutalOption struct {
	Enabled bool   `proxy:"enabled,omitempty"`
	Up      string `proxy:"up,omitempty"`
	Down    string `proxy:"down,omitempty"`
}

func (s *SingMux) DialContext(ctx context.Context, metadata *C.Metadata) (_ C.Conn, err error) {
	client, err := s.currentClient()
	if err != nil {
		return nil, err
	}
	c, err := client.DialContext(ctx, "tcp", M.ParseSocksaddrHostPort(metadata.String(), metadata.DstPort))
	if err != nil {
		return nil, err
	}
	return NewConn(c, s), err
}

func (s *SingMux) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (_ C.PacketConn, err error) {
	if s.onlyTcp {
		return s.ProxyAdapter.ListenPacketContext(ctx, metadata)
	}
	if err = s.ProxyAdapter.ResolveUDP(ctx, metadata); err != nil {
		return nil, err
	}
	client, err := s.currentClient()
	if err != nil {
		return nil, err
	}
	pc, err := client.ListenPacket(ctx, M.SocksaddrFromNet(metadata.UDPAddr()))
	if err != nil {
		return nil, err
	}
	if pc == nil {
		return nil, E.New("packetConn is nil")
	}
	return newPacketConn(N.NewThreadSafePacketConn(pc), s), nil
}

func (s *SingMux) SupportUDP() bool {
	if s.onlyTcp {
		return s.ProxyAdapter.SupportUDP()
	}
	return true
}

func (s *SingMux) SupportUOT() bool {
	if s.onlyTcp {
		return s.ProxyAdapter.SupportUOT()
	}
	return true
}

func (s *SingMux) ProxyInfo() C.ProxyInfo {
	info := s.ProxyAdapter.ProxyInfo()
	info.SMUX = true
	return info
}

// Close implements C.ProxyAdapter
func (s *SingMux) Close() error {
	s.clientMu.Lock()
	s.closed = true
	client := s.client
	s.client = nil
	s.clientMu.Unlock()
	if client != nil {
		_ = client.Close()
	}
	return s.ProxyAdapter.Close()
}

func (s *SingMux) currentClient() (*mux.Client, error) {
	s.clientMu.RLock()
	defer s.clientMu.RUnlock()
	if s.client == nil || s.closed {
		return nil, errors.New("sing-mux client is closed")
	}
	return s.client, nil
}

func (s *SingMux) ResetNetworkState() error {
	if resetter, ok := s.ProxyAdapter.(C.NetworkStateResetter); ok {
		if err := resetter.ResetNetworkState(); err != nil {
			return err
		}
	}
	client, err := mux.NewClient(s.clientOptions)
	if err != nil {
		return err
	}

	s.clientMu.Lock()
	if s.closed {
		s.clientMu.Unlock()
		_ = client.Close()
		return nil
	}
	oldClient := s.client
	s.client = client
	s.clientMu.Unlock()
	if oldClient != nil {
		_ = oldClient.Close()
	}
	return nil
}

func NewSingMux(option SingMuxOption, proxy ProxyAdapter) (ProxyAdapter, error) {
	// TODO
	// "TCP Brutal is only supported on Linux-based systems"

	singDialer := proxydialer.NewSingDialer(proxydialer.New(proxy, option.Statistic))
	clientOptions := mux.Options{
		Dialer:         singDialer,
		Logger:         log.SingLogger,
		Protocol:       option.Protocol,
		MaxConnections: option.MaxConnections,
		MinStreams:     option.MinStreams,
		MaxStreams:     option.MaxStreams,
		Padding:        option.Padding,
		TCPTimeout:     C.DefaultTCPTimeout,
		Brutal: mux.BrutalOptions{
			Enabled:    option.BrutalOpts.Enabled,
			SendBPS:    utils.StringToBps(option.BrutalOpts.Up),
			ReceiveBPS: utils.StringToBps(option.BrutalOpts.Down),
		},
	}
	client, err := mux.NewClient(clientOptions)
	if err != nil {
		return nil, err
	}
	outbound := &SingMux{
		ProxyAdapter:  proxy,
		client:        client,
		clientOptions: clientOptions,
		onlyTcp:       option.OnlyTcp,
	}
	return outbound, nil
}
