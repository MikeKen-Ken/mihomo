package executor

import (
	"strings"

	"github.com/metacubex/mihomo/log"
)

// PatchRuntimeLogLevel 仅同步内核运行时日志等级，与 Hub PATCH /configs 中 log-level 行为一致，
// 不重新加载配置文件，避免触发整表重载与 url-test/fallback 等附带测速。
func PatchRuntimeLogLevel(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return false
	}

	mux.Lock()
	defer mux.Unlock()

	l, ok := log.LogLevelMapping[name]
	if !ok {
		return false
	}

	log.SetLevel(l)
	return true
}
