package route

import (
	"net/http"

	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	rulepkg "github.com/metacubex/mihomo/rules/provider"
	"github.com/metacubex/mihomo/tunnel"

	"github.com/metacubex/chi/render"
)

type ruleProviderPreviewRule struct {
	RuleType string `json:"ruleType"`
	Payload  string `json:"payload"`
	Policy   string `json:"policy"`
}

type ruleProviderPreview struct {
	Name     string                    `json:"name"`
	Behavior string                    `json:"behavior"`
	Policy   string                    `json:"policy"`
	Rules    []ruleProviderPreviewRule `json:"rules"`
}

func getRuleProviderPreview(w http.ResponseWriter, r *http.Request) {
	p := r.Context().Value(CtxKeyProvider).(P.RuleProvider)
	name := r.Context().Value(CtxKeyProviderName).(string)
	fallbackPolicy := firstRuleSetPolicyForProvider(name)
	entries := rulepkg.CollectRuleProviderPreview(p)
	rules := make([]ruleProviderPreviewRule, len(entries))
	for i, e := range entries {
		pl := e.Adapter
		if pl == "" {
			pl = fallbackPolicy
		}
		rules[i] = ruleProviderPreviewRule{
			RuleType: e.Type,
			Payload:  e.Payload,
			Policy:   pl,
		}
	}
	render.JSON(w, r, ruleProviderPreview{
		Name:     name,
		Behavior: p.Behavior().String(),
		Policy:   fallbackPolicy,
		Rules:    rules,
	})
}

func firstRuleSetPolicyForProvider(providerName string) string {
	for _, rule := range tunnel.Rules() {
		rr := rule
		if rw, ok := rr.(C.RuleWrapper); ok {
			rr = rw.Unwrap()
		}
		if rr.RuleType() == C.RuleSet && rr.Payload() == providerName {
			return rr.Adapter()
		}
	}
	return ""
}
