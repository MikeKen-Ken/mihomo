//go:build cmfa

package dns

import "github.com/metacubex/mihomo/log"

// requestCoreRestart 在软恢复无效时的 Android 行为。
//
// Android 核心为进程内运行（JNI/gomobile），无独立进程可重启；且核心生命周期内已频繁因网络切换
// 触发等效刷新（NotifyDnsChanged -> FlushCacheWithDefaultResolver）。因此此处不打印机器可读哨兵
// （无消费方，仅造成误导性日志），改为普通告警，便于诊断上游 DNS / 网络问题。
func requestCoreRestart() {
	log.Warnln("[DNS] 连续解析失败且自愈无效，请检查上游 DNS 或网络连通性")
}
