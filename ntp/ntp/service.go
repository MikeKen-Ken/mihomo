package ntp

import (
	"context"
	"sync"
	"time"

	"github.com/metacubex/mihomo/component/dialer"
	"github.com/metacubex/mihomo/component/proxydialer"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	mihomoNtp "github.com/metacubex/mihomo/ntp"

	M "github.com/metacubex/sing/common/metadata"
	"github.com/metacubex/sing/common/ntp"
)

var globalSrv *Service
var globalMu sync.Mutex

type Service struct {
	server         M.Socksaddr
	dialer         proxydialer.SingDialer
	ticker         *time.Ticker
	ctx            context.Context
	cancel         context.CancelFunc
	syncSystemTime bool
}

func ReCreateNTPService(server string, interval time.Duration, dialerProxy string, tunnel C.Tunnel, syncSystemTime bool) {
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalSrv != nil {
		globalSrv.Stop()
	}
	if server == "" || interval <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	var cDialer C.Dialer = dialer.NewDialer()
	if dialerProxy != "" {
		cDialer = proxydialer.NewByName(dialerProxy, tunnel)
	}
	globalSrv = &Service{
		server:         M.ParseSocksaddr(server),
		dialer:         proxydialer.NewSingDialer(cDialer),
		ticker:         time.NewTicker(interval * time.Minute),
		ctx:            ctx,
		cancel:         cancel,
		syncSystemTime: syncSystemTime,
	}
	globalSrv.Start()
}

func (srv *Service) Start() {
	log.Infoln("NTP 服务启动，同步系统时间: %t", srv.syncSystemTime)
	go srv.loopUpdate()
}

func (srv *Service) Stop() {
	log.Infoln("NTP 服务停止")
	srv.cancel()
}

func (srv *Service) update() error {
	var response *ntp.Response
	var err error
	for i := 0; i < 3; i++ {
		response, err = ntp.Exchange(srv.ctx, srv.dialer, srv.server)
		if err != nil {
			if srv.ctx.Err() != nil {
				return nil
			}
			continue
		}
		offset := response.ClockOffset
		if offset > time.Duration(0) {
			log.Infoln("系统时钟比 NTP 快 %s", offset)
		} else if offset < time.Duration(0) {
			log.Infoln("系统时钟比 NTP 慢 %s", -offset)
		}
		mihomoNtp.SetOffset(offset)
		if srv.syncSystemTime {
			timeNow := response.Time
			syncErr := setSystemTime(timeNow)
			if syncErr == nil {
				log.Infoln("同步系统时间成功: %s", timeNow.Local().Format(ntp.TimeLayout))
			} else {
				log.Errorln("写入系统时间失败: %s", syncErr)
				srv.syncSystemTime = false
			}
		}
		return nil
	}
	return err
}

func (srv *Service) loopUpdate() {
	defer mihomoNtp.SetOffset(0)
	defer srv.ticker.Stop()
	for {
		err := srv.update()
		if err != nil {
			log.Warnln("同步时间失败: %s", err)
		}
		select {
		case <-srv.ctx.Done():
			return
		case <-srv.ticker.C:
		}
	}
}
