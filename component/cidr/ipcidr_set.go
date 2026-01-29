package cidr

import (
	"fmt"
	"net/netip"
	"unsafe"

	"go4.org/netipx"
)

type IpCidrSet struct {
	// must same with netipx.IPSet
	rr []netipx.IPRange
}

func NewIpCidrSet() *IpCidrSet {
	return &IpCidrSet{}
}

func (set *IpCidrSet) AddIpCidrForString(ipCidr string) error {
	prefix, err := netip.ParsePrefix(ipCidr)
	if err != nil {
		return err
	}
	return set.AddIpCidr(prefix)
}

func (set *IpCidrSet) AddIpCidr(ipCidr netip.Prefix) (err error) {
	if r := netipx.RangeOfPrefix(ipCidr); r.IsValid() {
		set.rr = append(set.rr, r)
	} else {
		err = fmt.Errorf("not valid ipcidr range: %s", ipCidr)
	}
	return
}

func (set *IpCidrSet) IsContainForString(ipString string) bool {
	ip, err := netip.ParseAddr(ipString)
	if err != nil {
		return false
	}
	return set.IsContain(ip)
}

func (set *IpCidrSet) IsContain(ip netip.Addr) bool {
	return set.ToIPSet().Contains(ip.WithZone(""))
}

// IsContainWithPrefix checks if the IP is contained in the set and returns the matching prefix.
func (set *IpCidrSet) IsContainWithPrefix(ip netip.Addr) (bool, netip.Prefix) {
	ip = ip.WithZone("")
	ipSet := set.ToIPSet()
	if !ipSet.Contains(ip) {
		return false, netip.Prefix{}
	}
	// Find the matching range and return its prefix
	for _, r := range set.rr {
		if r.Contains(ip) {
			// Return the first matching prefix from this range
			for _, prefix := range r.Prefixes() {
				if prefix.Contains(ip) {
					return true, prefix
				}
			}
		}
	}
	return true, netip.Prefix{}
}

// MatchIp implements C.IpMatcher
func (set *IpCidrSet) MatchIp(ip netip.Addr) bool {
	if set.IsEmpty() {
		return false
	}
	return set.IsContain(ip)
}

func (set *IpCidrSet) Merge() error {
	var b netipx.IPSetBuilder
	b.AddSet(set.ToIPSet())
	i, err := b.IPSet()
	if err != nil {
		return err
	}
	set.fromIPSet(i)
	return nil
}

func (set *IpCidrSet) IsEmpty() bool {
	return set == nil || len(set.rr) == 0
}

func (set *IpCidrSet) Foreach(f func(prefix netip.Prefix) bool) {
	for _, r := range set.rr {
		for _, prefix := range r.Prefixes() {
			if !f(prefix) {
				return
			}
		}
	}
}

// ToIPSet not safe convert to *netipx.IPSet
// be careful, must be used after Merge
func (set *IpCidrSet) ToIPSet() *netipx.IPSet {
	return (*netipx.IPSet)(unsafe.Pointer(set))
}

func (set *IpCidrSet) fromIPSet(i *netipx.IPSet) {
	*set = *(*IpCidrSet)(unsafe.Pointer(i))
}
