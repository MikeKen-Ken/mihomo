package provider

import (
	"errors"
	"io"
	"net/netip"

	"github.com/metacubex/mihomo/component/cidr"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"

	"go4.org/netipx"
)

type ipcidrStrategy struct {
	count   int
	cidrSet *cidr.IpCidrSet
	//trie    *trie.IpCidrTrie
}

func (i *ipcidrStrategy) Behavior() P.RuleBehavior {
	return P.IPCIDR
}

func (i *ipcidrStrategy) Match(metadata *C.Metadata, helper C.RuleMatchHelper) (bool, string) {
	if helper.ResolveIP != nil {
		helper.ResolveIP()
	}
	if i.cidrSet == nil {
		return false, ""
	}
	matched, prefix := i.cidrSet.IsContainWithPrefix(metadata.DstIP)
	if matched {
		if prefix.IsValid() {
			return true, "IP-CIDR," + prefix.String()
		}
		return true, "IP-CIDR," + metadata.DstIP.String()
	}
	return false, ""
}

func (i *ipcidrStrategy) Count() int {
	return i.count
}

func (i *ipcidrStrategy) Reset() {
	// i.trie = trie.NewIpCidrTrie()
	i.cidrSet = cidr.NewIpCidrSet()
	i.count = 0
}

func (i *ipcidrStrategy) Insert(rule string) {
	//err := i.trie.AddIpCidrForString(rule)
	err := i.cidrSet.AddIpCidrForString(rule)
	if err != nil {
		log.Warnln("无效的 IPCIDR:[%s]", rule)
	} else {
		i.count++
	}
}

func (i *ipcidrStrategy) FinishInsert() {
	i.cidrSet.Merge()
}

func (i *ipcidrStrategy) FromMrs(r io.Reader, count int) error {
	cidrSet, err := cidr.ReadIpCidrSet(r)
	if err != nil {
		return err
	}
	i.count = count
	i.cidrSet = cidrSet
	return nil
}

func (i *ipcidrStrategy) WriteMrs(w io.Writer) error {
	if i.cidrSet == nil {
		return errors.New("nil cidrSet")
	}
	return i.cidrSet.WriteBin(w)
}

func (i *ipcidrStrategy) DumpMrs(f func(key string) bool) {
	if i.cidrSet != nil {
		i.cidrSet.Foreach(func(prefix netip.Prefix) bool {
			return f(prefix.String())
		})
	}
}

func (i *ipcidrStrategy) ToIpCidr() *netipx.IPSet {
	return i.cidrSet.ToIPSet()
}

func NewIPCidrStrategy() *ipcidrStrategy {
	return &ipcidrStrategy{}
}
