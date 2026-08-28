//go:build cmfa

package dns

import "github.com/metacubex/mihomo/log"

// requestCoreRestart 在 Android 上请求进程内的完整网络恢复。
// Android 核心没有可单独重启的 sidecar，因此由 native 层关闭旧连接并重建持久会话。
func requestCoreRestart() {
	if requestNetworkRecovery("DNS automatic recovery was ineffective") {
		return
	}
	log.Warnln("[DNS] 连续解析失败且自愈无效，完整网络恢复回调未安装")
}
