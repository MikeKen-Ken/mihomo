package provider

import (
	"net/netip"

	P "github.com/metacubex/mihomo/constant/provider"
	"golang.org/x/exp/slices"
)

// PreviewEntry 单条规则集的展开项。Adapter 在 classical 行为下多为单条出站，其余行为通常为空。
type PreviewEntry struct {
	Type    string
	Payload string
	Adapter string
}

// CollectRuleProviderPreview 从已加载的规则提供者 strategy 中枚举全部规则行（运行时状态）。
func CollectRuleProviderPreview(p P.RuleProvider) []PreviewEntry {
	if p == nil {
		return nil
	}
	st := p.Strategy()
	switch v := st.(type) {
	case *domainStrategy:
		return v.previewEntries()
	case *ipcidrStrategy:
		return v.previewEntries()
	case *classicalStrategy:
		return v.previewEntries()
	default:
		return nil
	}
}

func (d *domainStrategy) previewEntries() []PreviewEntry {
	if d.domainSet == nil {
		return nil
	}
	var keys []string
	d.domainSet.Foreach(func(key string) bool {
		keys = append(keys, key)
		return true
	})
	slices.Sort(keys)
	out := make([]PreviewEntry, 0, len(keys))
	for _, key := range keys {
		if _, ok := slices.BinarySearch(keys, "+."+key); ok {
			continue
		}
		out = append(out, PreviewEntry{Type: "DOMAIN-SUFFIX", Payload: key})
	}
	return out
}

func (i *ipcidrStrategy) previewEntries() []PreviewEntry {
	if i.cidrSet == nil {
		return nil
	}
	var out []PreviewEntry
	i.cidrSet.Foreach(func(prefix netip.Prefix) bool {
		out = append(out, PreviewEntry{Type: "IP-CIDR", Payload: prefix.String()})
		return true
	})
	return out
}

func (c *classicalStrategy) previewEntries() []PreviewEntry {
	out := make([]PreviewEntry, 0, len(c.rules))
	for _, rule := range c.rules {
		out = append(out, PreviewEntry{
			Type:    rule.RuleType().String(),
			Payload: rule.Payload(),
			Adapter: rule.Adapter(),
		})
	}
	return out
}
