package dns

import (
	"context"
	"net"

	"github.com/metacubex/mihomo/adapter/inbound"
	"github.com/metacubex/mihomo/common/sockopt"
	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/log"

	D "github.com/miekg/dns"
)

var (
	address string
	server  = &Server{}

	dnsDefaultTTL uint32 = 600
)

type Server struct {
	service   resolver.Service
	tcpServer *D.Server
	udpServer *D.Server
}

// ServeDNS implement D.Handler ServeDNS
func (s *Server) ServeDNS(w D.ResponseWriter, r *D.Msg) {
	msg, err := s.service.ServeMsg(context.Background(), r)
	if err != nil {
		m := new(D.Msg)
		m.SetRcode(r, D.RcodeServerFailure)
		// does not matter if this write fails
		w.WriteMsg(m)
		return
	}
	msg.Compress = true
	w.WriteMsg(msg)
}

func (s *Server) SetService(service resolver.Service) {
	s.service = service
}

func ReCreateServer(addr string, service resolver.Service) {
	if addr == address && service != nil {
		server.SetService(service)
		return
	}

	if server.tcpServer != nil {
		_ = server.tcpServer.Shutdown()
		server.tcpServer = nil
	}

	if server.udpServer != nil {
		_ = server.udpServer.Shutdown()
		server.udpServer = nil
	}

	server.service = nil
	address = ""

	if addr == "" || service == nil {
		return
	}

	var err error
	defer func() {
		if err != nil {
			log.Errorln("启动 DNS 服务失败: %s", err.Error())
		}
	}()

	_, port, err := net.SplitHostPort(addr)
	if port == "0" || port == "" || err != nil {
		return
	}

	address = addr
	server = &Server{service: service}

	go func() {
		p, err := inbound.ListenPacket("udp", addr)
		if err != nil {
			log.Errorln("启动 DNS 服务(UDP)失败: %s", err.Error())
			return
		}

		if err := sockopt.UDPReuseaddr(p); err != nil {
			log.Warnln("复用 UDP 地址失败: %s", err)
		}

		log.Infoln("DNS 服务(UDP)监听地址: %s", p.LocalAddr().String())
		server.udpServer = &D.Server{Addr: addr, PacketConn: p, Handler: server}
		_ = server.udpServer.ActivateAndServe()
	}()

	go func() {
		l, err := inbound.Listen("tcp", addr)
		if err != nil {
			log.Errorln("启动 DNS 服务(TCP)失败: %s", err.Error())
			return
		}

		log.Infoln("DNS 服务(TCP)监听地址: %s", l.Addr().String())
		server.tcpServer = &D.Server{Addr: addr, Listener: l, Handler: server}
		_ = server.tcpServer.ActivateAndServe()
	}()

}
