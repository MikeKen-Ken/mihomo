//go:build !cmfa

package dns

import "github.com/metacubex/mihomo/log"

// dnsRestartSentinel 是请求上层（clash-verge-rev 桌面端）重启核心进程的稳定标记。
// 上层通过匹配该 ASCII 子串触发进程级重启；MUST NOT 随意修改字符串内容（与上层解析约定一致）。
const dnsRestartSentinel = "[APP] dns-stall-unrecoverable request-core-restart"

// requestCoreRestart 在软恢复无效时请求上层重启核心进程（独立 sidecar 进程，重启干净彻底）。
// ERROR 级别确保非 silent 日志配置下也能送达 stdout，被上层日志管道捕获。
func requestCoreRestart() {
	log.Errorln(dnsRestartSentinel)
}
