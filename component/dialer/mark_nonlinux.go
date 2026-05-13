//go:build !linux

package dialer

import (
	"net"
	"net/netip"
	"sync"

	"github.com/metacubex/mihomo/log"
)

var printMarkWarnOnce sync.Once

func printMarkWarn() {
	printMarkWarnOnce.Do(func() {
		log.Warnln("当前平台不支持套接字路由标记 (routing-mark)")
	})
}

func bindMarkToDialer(mark int, dialer *net.Dialer, _ string, _ netip.Addr) {
	printMarkWarn()
}

func bindMarkToListenConfig(mark int, lc *net.ListenConfig, _, _ string) {
	printMarkWarn()
}
