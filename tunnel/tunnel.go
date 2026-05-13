package tunnel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	syncatomic "sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/loopback"
	"github.com/metacubex/mihomo/component/nat"
	"github.com/metacubex/mihomo/component/process"
	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/component/slowdown"
	"github.com/metacubex/mihomo/component/sniffer"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/constant/features"
	P "github.com/metacubex/mihomo/constant/provider"
	icontext "github.com/metacubex/mihomo/context"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

const (
	queueCapacity  = 64  // chan capacity tcpQueue and udpQueue
	senderCapacity = 128 // chan capacity of PacketSender
)

var (
	status        = atomic.NewInt32Enum(Suspend)
	udpInit       sync.Once
	udpQueues     []chan C.PacketAdapter
	natTable      = nat.New()
	rules         []C.Rule
	listeners     = make(map[string]C.InboundListener)
	subRules      map[string][]C.Rule
	proxies       = make(map[string]C.Proxy)
	providers     map[string]P.ProxyProvider
	ruleProviders map[string]P.RuleProvider
	configMux     sync.RWMutex

	// for compatibility, lazy init
	tcpQueue  chan C.ConnContext
	tcpInOnce sync.Once
	udpQueue  chan C.PacketAdapter
	udpInOnce sync.Once

	// Outbound Rule
	mode = Rule

	// default timeout for UDP session
	udpTimeout = 60 * time.Second

	findProcessMode = atomic.NewInt32Enum(process.FindProcessStrict)
	lanMaxDevices   int32
	// 0 = reject, 1 = drop
	lanOverLimitAction int32
	lanDeviceLimitCache = struct {
		sync.Mutex
		expires time.Time
		limit   int
		devices map[netip.Addr]struct{}
	}{}

	snifferDispatcher *sniffer.Dispatcher
	sniffingEnable    = false

	ruleUpdateCallback = utils.NewCallback[P.RuleProvider]()
)

const lanDeviceLimitCacheTTL = time.Second

type tunnel struct{}

var Tunnel = tunnel{}
var _ C.Tunnel = Tunnel
var _ P.Tunnel = Tunnel

func (t tunnel) HandleTCPConn(conn net.Conn, metadata *C.Metadata) {
	connCtx := icontext.NewConnContext(conn, metadata)
	handleTCPConn(connCtx)
}

func initUDP() {
	numUDPWorkers := 4
	if num := runtime.GOMAXPROCS(0); num > numUDPWorkers {
		numUDPWorkers = num
	}

	udpQueues = make([]chan C.PacketAdapter, numUDPWorkers)
	for i := 0; i < numUDPWorkers; i++ {
		queue := make(chan C.PacketAdapter, queueCapacity)
		udpQueues[i] = queue
		go processUDP(queue)
	}
}

func (t tunnel) HandleUDPPacket(packet C.UDPPacket, metadata *C.Metadata) {
	udpInit.Do(initUDP)

	packetAdapter := C.NewPacketAdapter(packet, metadata)
	key := packetAdapter.Key()

	hash := utils.MapHash(key)
	queueNo := uint(hash) % uint(len(udpQueues))

	select {
	case udpQueues[queueNo] <- packetAdapter:
	default:
		packet.Drop()
	}
}

func (t tunnel) NatTable() C.NatTable {
	return natTable
}

func (t tunnel) Providers() map[string]P.ProxyProvider {
	return providers
}

func (t tunnel) RuleProviders() map[string]P.RuleProvider {
	return ruleProviders
}

func (t tunnel) RuleUpdateCallback() *utils.Callback[P.RuleProvider] {
	return ruleUpdateCallback
}

func OnSuspend() {
	status.Store(Suspend)
}

func OnInnerLoading() {
	status.Store(Inner)
}

func OnRunning() {
	status.Store(Running)
}

func Status() TunnelStatus {
	return status.Load()
}

func SetSniffing(b bool) {
	if snifferDispatcher.Enable() {
		configMux.Lock()
		sniffingEnable = b
		configMux.Unlock()
	}
}

func IsSniffing() bool {
	return sniffingEnable
}

// TCPIn return fan-in queue
// Deprecated: using Tunnel instead
func TCPIn() chan<- C.ConnContext {
	tcpInOnce.Do(func() {
		tcpQueue = make(chan C.ConnContext, queueCapacity)
		go func() {
			for connCtx := range tcpQueue {
				go handleTCPConn(connCtx)
			}
		}()
	})
	return tcpQueue
}

// UDPIn return fan-in udp queue
// Deprecated: using Tunnel instead
func UDPIn() chan<- C.PacketAdapter {
	udpInOnce.Do(func() {
		udpQueue = make(chan C.PacketAdapter, queueCapacity)
		go func() {
			for packet := range udpQueue {
				Tunnel.HandleUDPPacket(packet, packet.Metadata())
			}
		}()
	})
	return udpQueue
}

// NatTable return nat table
func NatTable() C.NatTable {
	return natTable
}

// Rules return all rules
func Rules() []C.Rule {
	return rules
}

func Listeners() map[string]C.InboundListener {
	return listeners
}

// UpdateRules handle update rules
func UpdateRules(newRules []C.Rule, newSubRule map[string][]C.Rule, rp map[string]P.RuleProvider) {
	configMux.Lock()
	rules = newRules
	ruleProviders = rp
	subRules = newSubRule
	configMux.Unlock()
}

// Proxies return all proxies
func Proxies() map[string]C.Proxy {
	return proxies
}

// Providers return all compatible providers
func Providers() map[string]P.ProxyProvider {
	return providers
}

// RuleProviders return all loaded rule providers
func RuleProviders() map[string]P.RuleProvider {
	return ruleProviders
}

// UpdateProxies handle update proxies
func UpdateProxies(newProxies map[string]C.Proxy, newProviders map[string]P.ProxyProvider) {
	configMux.Lock()
	proxies = newProxies
	providers = newProviders
	configMux.Unlock()
}

func UpdateListeners(newListeners map[string]C.InboundListener) {
	configMux.Lock()
	defer configMux.Unlock()
	listeners = newListeners
}

func UpdateSniffer(dispatcher *sniffer.Dispatcher) {
	configMux.Lock()
	snifferDispatcher = dispatcher
	sniffingEnable = dispatcher.Enable()
	configMux.Unlock()
}

// Mode return current mode
func Mode() TunnelMode {
	return mode
}

// SetMode change the mode of tunnel
func SetMode(m TunnelMode) {
	mode = m
}

func FindProcessMode() process.FindProcessMode {
	return findProcessMode.Load()
}

// SetFindProcessMode replace SetAlwaysFindProcess
// always find process info if legacyAlways = true or mode.Always() = true, may be increase many memory
func SetFindProcessMode(mode process.FindProcessMode) {
	findProcessMode.Store(mode)
}

func SetLanDeviceLimit(limit int, action string) {
	if limit < 0 {
		limit = 0
	}
	syncatomic.StoreInt32(&lanMaxDevices, int32(limit))
	invalidateLanDeviceLimitCache()
	if strings.EqualFold(strings.TrimSpace(action), "drop") {
		syncatomic.StoreInt32(&lanOverLimitAction, 1)
		return
	}
	syncatomic.StoreInt32(&lanOverLimitAction, 0)
}

func invalidateLanDeviceLimitCache() {
	lanDeviceLimitCache.Lock()
	lanDeviceLimitCache.expires = time.Time{}
	lanDeviceLimitCache.limit = 0
	lanDeviceLimitCache.devices = nil
	lanDeviceLimitCache.Unlock()
}

func LanMaxDevices() int {
	return int(syncatomic.LoadInt32(&lanMaxDevices))
}

func LanOverLimitAction() string {
	if syncatomic.LoadInt32(&lanOverLimitAction) == 1 {
		return "drop"
	}
	return "reject"
}

func isLanSourceIP(addr netip.Addr) bool {
	if !addr.IsValid() {
		return false
	}
	addr = addr.Unmap()
	if addr.IsLoopback() || addr.IsUnspecified() || addr.IsMulticast() {
		return false
	}
	if addr.IsPrivate() {
		return true
	}
	// IPv6 link-local can also represent LAN devices.
	return addr.Is6() && addr.IsLinkLocalUnicast()
}

func checkLanDeviceLimit(sourceIP netip.Addr) bool {
	limit := int(syncatomic.LoadInt32(&lanMaxDevices))
	if limit <= 0 || !isLanSourceIP(sourceIP) {
		return true
	}
	sourceIP = sourceIP.Unmap()

	now := time.Now()
	lanDeviceLimitCache.Lock()
	if lanDeviceLimitCache.devices != nil &&
		lanDeviceLimitCache.limit == limit &&
		now.Before(lanDeviceLimitCache.expires) {
		if _, ok := lanDeviceLimitCache.devices[sourceIP]; ok {
			lanDeviceLimitCache.Unlock()
			return true
		}
		if len(lanDeviceLimitCache.devices) < limit {
			lanDeviceLimitCache.devices[sourceIP] = struct{}{}
			lanDeviceLimitCache.Unlock()
			return true
		}
		lanDeviceLimitCache.Unlock()
		return false
	}
	lanDeviceLimitCache.Unlock()

	deviceSet := make(map[netip.Addr]struct{}, limit+1)
	alreadyTracked := false
	statistic.DefaultManager.Range(func(c statistic.Tracker) bool {
		info := c.Info()
		if info == nil || info.Metadata == nil {
			return true
		}
		ip := info.Metadata.SrcIP.Unmap()
		if !isLanSourceIP(ip) {
			return true
		}
		deviceSet[ip] = struct{}{}
		if ip == sourceIP {
			alreadyTracked = true
		}
		return true
	})
	if alreadyTracked {
		lanDeviceLimitCache.Lock()
		lanDeviceLimitCache.expires = now.Add(lanDeviceLimitCacheTTL)
		lanDeviceLimitCache.limit = limit
		lanDeviceLimitCache.devices = deviceSet
		lanDeviceLimitCache.Unlock()
		return true
	}
	allowed := len(deviceSet) < limit
	if allowed {
		deviceSet[sourceIP] = struct{}{}
	}
	lanDeviceLimitCache.Lock()
	lanDeviceLimitCache.expires = now.Add(lanDeviceLimitCacheTTL)
	lanDeviceLimitCache.limit = limit
	lanDeviceLimitCache.devices = deviceSet
	lanDeviceLimitCache.Unlock()
	return allowed
}

func logLanDeviceOverLimit(metadata *C.Metadata) {
	if syncatomic.LoadInt32(&lanOverLimitAction) == 1 {
		log.Warnln("[局域网设备限制] 丢弃超限设备 source=%s limit=%d", metadata.SourceAddress(), LanMaxDevices())
		return
	}
	log.Warnln("[局域网设备限制] 拒绝超限设备 source=%s limit=%d", metadata.SourceAddress(), LanMaxDevices())
}

func isHandle(t C.Type) bool {
	status := status.Load()
	return status == Running || (status == Inner && t == C.INNER)
}

func fixMetadata(metadata *C.Metadata) {
	// first unmap dstIP
	metadata.DstIP = metadata.DstIP.Unmap()
	// handle IP string on host
	if ip, err := netip.ParseAddr(metadata.Host); err == nil {
		metadata.DstIP = ip.Unmap()
		metadata.Host = ""
	}
}

func needLookupIP(metadata *C.Metadata) bool {
	return resolver.MappingEnabled() && metadata.Host == "" && metadata.DstIP.IsValid()
}

func preHandleMetadata(metadata *C.Metadata) error {
	// preprocess enhanced-mode metadata
	if needLookupIP(metadata) {
		host, exist := resolver.FindHostByIP(metadata.DstIP)
		if exist {
			metadata.Host = host
			metadata.DNSMode = C.DNSMapping
			if resolver.IsFakeIP(metadata.DstIP) {
				// only clear dstIP if it is confirmed to be a fake IP
				metadata.DstIP = netip.Addr{}
				metadata.DNSMode = C.DNSFakeIP
			} else if node, ok := resolver.DefaultHosts.Search(host, false); ok {
				// redir-host should lookup the hosts
				metadata.DstIP, _ = node.RandIP()
			} else if node != nil && node.IsDomain {
				metadata.Host = node.Domain
			}
		} else if resolver.IsFakeIP(metadata.DstIP) {
			return fmt.Errorf("fake DNS record %s missing", metadata.DstIP)
		}
	} else if node, ok := resolver.DefaultHosts.Search(metadata.Host, true); ok {
		// try use domain mapping
		metadata.Host = node.Domain
	}

	return nil
}

func resolveMetadata(metadata *C.Metadata) (proxy C.Proxy, rule C.Rule, err error) {
	if metadata.SpecialProxy != "" {
		var exist bool
		proxy, exist = proxies[metadata.SpecialProxy]
		if !exist {
			err = fmt.Errorf("proxy %s not found", metadata.SpecialProxy)
		}
		return
	}
	var (
		resolved             bool
		attemptProcessLookup = metadata.Type != C.INNER
	)

	if node, ok := resolver.DefaultHosts.Search(metadata.Host, false); ok {
		metadata.DstIP, _ = node.RandIP()
		resolved = true
	}

	helper := C.RuleMatchHelper{
		ResolveIP: func() {
			if !resolved && metadata.Host != "" && !metadata.Resolved() {
				ctx, cancel := context.WithTimeout(context.Background(), resolver.DefaultDNSTimeout)
				defer cancel()
				ip, err := resolver.ResolveIP(ctx, metadata.Host)
				if err != nil {
					log.Debugln("[DNS] 解析 %s 失败: %s", metadata.Host, err.Error())
				} else {
					log.Debugln("[DNS] %s --> %s", metadata.Host, ip.String())
					metadata.DstIP = ip
				}
				resolved = true
			}
		},
		FindProcess: func() {
			if attemptProcessLookup {
				attemptProcessLookup = false
				if !features.CMFA {
					// normal check for process
					uid, path, err := process.FindProcessName(metadata.NetWork.String(), metadata.SrcIP, int(metadata.SrcPort))
					if err != nil {
						log.Debugln("[进程] 查找进程失败 %s: %v", metadata.String(), err)
					} else {
						metadata.Process = filepath.Base(path)
						metadata.ProcessPath = path
						metadata.Uid = uid

						if pkg, err := process.FindPackageName(metadata); err == nil { // for android (not CMFA) package names
							metadata.Process = pkg
						}
					}
				} else {
					// check package names
					pkg, err := process.FindPackageName(metadata)
					if err != nil {
						log.Debugln("[进程] 查找进程失败 %s: %v", metadata.String(), err)
					} else {
						metadata.Process = pkg
					}
				}
			}
		},
	}

	switch FindProcessMode() {
	case process.FindProcessAlways:
		helper.FindProcess()
		helper.FindProcess = nil
	case process.FindProcessOff:
		helper.FindProcess = nil
	}

	switch mode {
	case Direct:
		proxy = proxies["DIRECT"]
	case Global:
		proxy = proxies["GLOBAL"]
	// Rule
	default:
		proxy, rule, err = match(metadata, helper)
	}
	return
}

// processUDP starts a loop to handle udp packet
func processUDP(queue chan C.PacketAdapter) {
	for conn := range queue {
		handleUDPConn(conn)
	}
}

func countProxyRequest(proxy C.Proxy, metadata *C.Metadata) {
	countProxyRequestRecursive(proxy, metadata, map[string]struct{}{})
}

func countProxyRequestRecursive(proxy C.Proxy, metadata *C.Metadata, seen map[string]struct{}) {
	if proxy == nil {
		return
	}
	name := proxy.Name()
	if _, ok := seen[name]; ok {
		return
	}
	seen[name] = struct{}{}

	adapter := proxy.Adapter()
	if counter, ok := adapter.(C.RequestCounter); ok {
		counter.CountRequest(metadata)
	}
	countProxyRequestRecursive(adapter.Unwrap(metadata, false), metadata, seen)
}

func handleUDPConn(packet C.PacketAdapter) {
	if !isHandle(packet.Metadata().Type) {
		packet.Drop()
		return
	}

	metadata := packet.Metadata()
	if !metadata.Valid() {
		packet.Drop()
		log.Warnln("[元数据] 无效: %#v", metadata)
		return
	}
	fixMetadata(metadata) // fix some metadata not set via metadata.SetRemoteAddr or metadata.SetRemoteAddress

	if err := preHandleMetadata(metadata.Clone()); err != nil { // precheck without modify metadata
		packet.Drop()
		log.Debugln("[元数据预处理] 错误: %s", err)
		return
	}
	key := packet.Key()
	sender, loaded := natTable.GetOrCreate(key, func() C.PacketSender {
		sender := newPacketSender()
		if sniffingEnable && snifferDispatcher.Enable() {
			return snifferDispatcher.UDPSniff(packet, sender)
		}
		return sender
	})
	if !loaded {
		// For UDP, check device limit only when creating a new NAT session,
		// avoiding per-packet scans under high PPS traffic.
		if !checkLanDeviceLimit(metadata.SrcIP) {
			packet.Drop()
			logLanDeviceOverLimit(metadata)
			sender.Close()
			natTable.Delete(key)
			return
		}
		dial := func() (C.PacketConn, C.WriteBackProxy, error) {
			originMetadata := metadata  // save origin metadata
			metadata = metadata.Clone() // don't modify PacketAdapter's metadata

			if err := sender.DoSniff(metadata); err != nil {
				log.Warnln("[UDP] 嗅探失败: %s", err.Error())
				return nil, nil, err
			}

			_ = preHandleMetadata(metadata) // error was pre-checked

			proxy, rule, err := resolveMetadata(metadata)
			if err != nil {
				log.Warnln("[UDP] 解析元数据失败: %s", err.Error())
				return nil, nil, err
			}
			countProxyRequest(proxy, metadata)

			dialMetadata := metadata.Pure()
			ctx, cancel := context.WithTimeout(context.Background(), C.DefaultUDPTimeout)
			defer cancel()
			rawPc, err := retry(ctx, func(ctx context.Context) (C.PacketConn, error) {
				return proxy.ListenPacketContext(ctx, dialMetadata)
			}, func(err error) {
				logMetadataErr(metadata, rule, proxy, err)
			})
			if err != nil {
				return nil, nil, err
			}
			logMetadata(metadata, rule, rawPc)

			pc := statistic.NewUDPTracker(rawPc, statistic.DefaultManager, metadata, rule, 0, 0, true)

			sender.AddMapping(originMetadata, dialMetadata)
			oAddrPort := dialMetadata.AddrPort()
			writeBackProxy := nat.NewWriteBackProxy(packet)

			go handleUDPToLocal(writeBackProxy, pc, sender, key, oAddrPort)
			return pc, writeBackProxy, nil
		}

		go func() {
			pc, proxy, err := dial()
			if err != nil {
				sender.Close()
				natTable.Delete(key)
				return
			}
			sender.Process(pc, proxy)
		}()
	}
	sender.Send(packet) // nonblocking
}

func handleTCPConn(connCtx C.ConnContext) {
	if !isHandle(connCtx.Metadata().Type) {
		_ = connCtx.Conn().Close()
		return
	}

	defer func(conn net.Conn) {
		_ = conn.Close()
	}(connCtx.Conn())

	metadata := connCtx.Metadata()
	if !metadata.Valid() {
		log.Warnln("[元数据] 无效: %#v", metadata)
		return
	}
	fixMetadata(metadata) // fix some metadata not set via metadata.SetRemoteAddr or metadata.SetRemoteAddress

	preHandleFailed := false
	if err := preHandleMetadata(metadata); err != nil {
		log.Debugln("[元数据预处理] 错误: %s", err)
		preHandleFailed = true
	}

	conn := connCtx.Conn()
	conn.ResetPeeked() // reset before sniffer
	if sniffingEnable && snifferDispatcher.Enable() {
		// Try to sniff a domain when `preHandleMetadata` failed, this is usually
		// caused by a "Fake DNS record missing" error when enhanced-mode is fake-ip.
		if snifferDispatcher.TCPSniff(conn, metadata) {
			// we now have a domain name
			preHandleFailed = false
		}
	}

	// If both trials have failed, we can do nothing but give up
	if preHandleFailed {
		log.Debugln("[元数据预处理] 无法为连接嗅探到域名 %s --> %s，放弃",
			metadata.SourceDetail(), metadata.RemoteAddress())
		return
	}
	if !checkLanDeviceLimit(metadata.SrcIP) {
		logLanDeviceOverLimit(metadata)
		return
	}

	peekMutex := sync.Mutex{}
	if !conn.Peeked() {
		peekMutex.Lock()
		go func() {
			defer peekMutex.Unlock()
			_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			_, _ = conn.Peek(1)
			_ = conn.SetReadDeadline(time.Time{})
		}()
	}

	proxy, rule, err := resolveMetadata(metadata)
	if err != nil {
		log.Warnln("[元数据] 解析失败: %s", err.Error())
		return
	}
	countProxyRequest(proxy, metadata)

	dialMetadata := metadata
	if len(metadata.Host) > 0 {
		if node, ok := resolver.DefaultHosts.Search(metadata.Host, false); ok {
			if dstIp, _ := node.RandIP(); !resolver.IsFakeIP(dstIp) {
				dialMetadata.DstIP = dstIp
				dialMetadata.DNSMode = C.DNSHosts
				dialMetadata = dialMetadata.Pure()
			}
		}
	}

	var peekBytes []byte
	var peekLen int

	ctx, cancel := context.WithTimeout(context.Background(), C.DefaultTCPTimeout)
	defer cancel()
	remoteConn, err := retry(ctx, func(ctx context.Context) (remoteConn C.Conn, err error) {
		remoteConn, err = proxy.DialContext(ctx, dialMetadata)
		if err != nil {
			return
		}

		if N.NeedHandshake(remoteConn) {
			defer func() {
				if err != nil {
					_ = remoteConn.Close()
					for _, chain := range remoteConn.Chains() {
						if chain == "REJECT" {
							err = nil
							return
						}
					}
					remoteConn = nil
				}
			}()
			peekMutex.Lock()
			defer peekMutex.Unlock()
			peekBytes, _ = conn.Peek(conn.Buffered())
			_, err = remoteConn.Write(peekBytes)
			if err != nil {
				return
			}
			if peekLen = len(peekBytes); peekLen > 0 {
				_, _ = conn.Discard(peekLen)
			}
		}
		return
	}, func(err error) {
		logMetadataErr(metadata, rule, proxy, err)
	})
	if err != nil {
		return
	}
	logMetadata(metadata, rule, remoteConn)

	remoteConn = statistic.NewTCPTracker(remoteConn, statistic.DefaultManager, metadata, rule, int64(peekLen), 0, true)
	defer func(remoteConn C.Conn) {
		_ = remoteConn.Close()
	}(remoteConn)

	_ = conn.SetReadDeadline(time.Now()) // stop unfinished peek
	peekMutex.Lock()
	defer peekMutex.Unlock()
	_ = conn.SetReadDeadline(time.Time{}) // reset
	handleSocket(conn, remoteConn)
}

func logMetadataErr(metadata *C.Metadata, rule C.Rule, proxy C.ProxyAdapter, err error) {
	if features.CMFA {
		logMetadataErrCMFA(metadata, rule, proxy, err)
		return
	}
	if rule == nil {
		log.Warnln("[%s] 拨号 %s %s --> %s 错误: %s", strings.ToUpper(metadata.NetWork.String()), proxy.Name(), metadata.SourceDetail(), metadata.RemoteAddress(), err.Error())
	} else {
		ruleInfo := formatRuleInfo(rule, metadata)
		log.Warnln("[%s] 拨号 %s (匹配 %s) %s --> %s 错误: %s", strings.ToUpper(metadata.NetWork.String()), proxy.Name(), ruleInfo, metadata.SourceDetail(), metadata.RemoteAddress(), err.Error())
	}
}

func logMetadata(metadata *C.Metadata, rule C.Rule, remoteConn C.Connection) {
	if features.CMFA {
		logMetadataCMFA(metadata, rule, remoteConn)
		return
	}
	switch {
	case metadata.SpecialProxy != "":
		log.Infoln("[%s] %s --> %s 使用 %s", strings.ToUpper(metadata.NetWork.String()), metadata.SourceDetail(), metadata.RemoteAddress(), remoteConn.Chains().String())
	case rule != nil:
		ruleInfo := formatRuleInfo(rule, metadata)
		log.Infoln("[%s] %s --> %s 匹配 %s 使用 %s", strings.ToUpper(metadata.NetWork.String()), metadata.SourceDetail(), metadata.RemoteAddress(), ruleInfo, remoteConn.Chains().String())
	case mode == Global:
		log.Infoln("[%s] %s --> %s 使用 GLOBAL", strings.ToUpper(metadata.NetWork.String()), metadata.SourceDetail(), metadata.RemoteAddress())
	case mode == Direct:
		log.Infoln("[%s] %s --> %s 使用 DIRECT", strings.ToUpper(metadata.NetWork.String()), metadata.SourceDetail(), metadata.RemoteAddress())
	default:
		log.Infoln("[%s] %s --> %s 未匹配任何规则，使用 %s", strings.ToUpper(metadata.NetWork.String()), metadata.SourceDetail(), metadata.RemoteAddress(), remoteConn.Chains().String())
	}
}

// formatRuleInfo formats the rule information for logging
// For RULE-SET, it includes both the rule-set name and the matched internal rule detail
func formatRuleInfo(rule C.Rule, metadata *C.Metadata) string {
	ruleType := rule.RuleType().String()
	payload := rule.Payload()
	detail := metadata.RuleDetail

	if payload == "" {
		if detail != "" {
			return fmt.Sprintf("%s[%s]", ruleType, detail)
		}
		return ruleType
	}

	if detail != "" {
		return fmt.Sprintf("%s(%s)[%s]", ruleType, payload, detail)
	}
	return fmt.Sprintf("%s(%s)", ruleType, payload)
}

// logMetadataCMFA formats log for Android (CMFA), plain text only (no ANSI; logcat shows raw codes)
// Format: [TCP] 127.0.0.1:54210 --> com.android.vending --> play-fe.googleapis.com:443 --> proxy --> DOMAIN-SUFFIX,+..googleapis.com --> 🔀[🇭🇰 01]
func logMetadataCMFA(metadata *C.Metadata, rule C.Rule, remoteConn C.Connection) {
	network := strings.ToUpper(metadata.NetWork.String())
	sourceAddr := metadata.SourceAddress()
	process := metadata.Process
	remoteAddr := metadata.RemoteAddress()
	chains := remoteConn.Chains().String()

	processPart := ""
	if process != "" {
		processPart = fmt.Sprintf(" --> %s", process)
	}

	switch {
	case metadata.SpecialProxy != "":
		log.Infoln("[%s] %s%s --> %s --> %s --> %s",
			network, sourceAddr, processPart, remoteAddr, metadata.SpecialProxy, chains)
	case rule != nil:
		ruleType, ruleDetail := formatRuleInfoCMFA(rule, metadata)
		ruleDetailPart := ""
		if ruleDetail != "" {
			ruleDetailPart = fmt.Sprintf(" --> %s", ruleDetail)
		}
		log.Infoln("[%s] %s%s --> %s --> %s%s --> %s",
			network, sourceAddr, processPart, remoteAddr, ruleType, ruleDetailPart, chains)
	case mode == Global:
		log.Infoln("[%s] %s%s --> %s --> 全局 --> %s",
			network, sourceAddr, processPart, remoteAddr, chains)
	case mode == Direct:
		log.Infoln("[%s] %s%s --> %s --> 直连 --> %s",
			network, sourceAddr, processPart, remoteAddr, chains)
	default:
		log.Infoln("[%s] %s%s --> %s --> 无匹配 --> %s",
			network, sourceAddr, processPart, remoteAddr, chains)
	}
}

// logMetadataErrCMFA formats error log for Android (CMFA), plain text only
func logMetadataErrCMFA(metadata *C.Metadata, rule C.Rule, proxy C.ProxyAdapter, err error) {
	network := strings.ToUpper(metadata.NetWork.String())
	sourceAddr := metadata.SourceAddress()
	process := metadata.Process
	remoteAddr := metadata.RemoteAddress()
	errMsg := err.Error()

	processPart := ""
	if process != "" {
		processPart = fmt.Sprintf(" --> %s", process)
	}

	if rule == nil {
		log.Warnln("[%s] %s%s --> %s --> %s --> 错误: %s",
			network, sourceAddr, processPart, remoteAddr, proxy.Name(), errMsg)
	} else {
		ruleType, ruleDetail := formatRuleInfoCMFA(rule, metadata)
		ruleDetailPart := ""
		if ruleDetail != "" {
			ruleDetailPart = fmt.Sprintf(" --> %s", ruleDetail)
		}
		log.Warnln("[%s] %s%s --> %s --> %s%s --> %s --> 错误: %s",
			network, sourceAddr, processPart, remoteAddr, ruleType, ruleDetailPart, proxy.Name(), errMsg)
	}
}

// formatRuleInfoCMFA returns rule type and rule detail separately for CMFA logging
func formatRuleInfoCMFA(rule C.Rule, metadata *C.Metadata) (ruleType string, ruleDetail string) {
	ruleType = rule.RuleType().String()
	payload := rule.Payload()
	detail := metadata.RuleDetail

	if payload != "" {
		ruleType = fmt.Sprintf("%s(%s)", ruleType, payload)
	}

	// Return the detail (which may include the domain suffix like +..googleapis.com)
	ruleDetail = detail
	return
}

func match(metadata *C.Metadata, helper C.RuleMatchHelper) (C.Proxy, C.Rule, error) {
	configMux.RLock()
	defer configMux.RUnlock()

	for _, rule := range getRules(metadata) {
		if matched, ada := rule.Match(metadata, helper); matched {
			adapter, ok := proxies[ada]
			if !ok {
				continue
			}

			// parse multi-layer nesting
			passed := false
			for adapter := adapter; adapter != nil; adapter = adapter.Unwrap(metadata, false) {
				if adapter.Type() == C.Pass {
					passed = true
					break
				}
			}
			if passed {
				log.Debugln("%s 匹配 Pass 规则", adapter.Name())
				continue
			}

			if metadata.NetWork == C.UDP && !adapter.SupportUDP() {
				log.Debugln("%s 不支持 UDP", adapter.Name())
				continue
			}

			return adapter, rule, nil
		}
	}

	return proxies["DIRECT"], nil, nil
}

func getRules(metadata *C.Metadata) []C.Rule {
	if sr, ok := subRules[metadata.SpecialRules]; ok {
		log.Debugln("[规则] 使用 %s 规则集", metadata.SpecialRules)
		return sr
	} else {
		log.Debugln("[规则] 使用默认规则")
		return rules
	}
}

func shouldStopRetry(err error) bool {
	if errors.Is(err, resolver.ErrIPNotFound) {
		return true
	}
	if errors.Is(err, resolver.ErrIPVersion) {
		return true
	}
	if errors.Is(err, resolver.ErrIPv6Disabled) {
		return true
	}
	if errors.Is(err, loopback.ErrReject) {
		return true
	}
	return false
}

func retry[T any](ctx context.Context, ft func(context.Context) (T, error), fe func(err error)) (t T, err error) {
	s := slowdown.New()
	for i := 0; i < 10; i++ {
		t, err = ft(ctx)
		if err != nil {
			if fe != nil {
				fe(err)
			}
			if shouldStopRetry(err) {
				return
			}
			if s.Wait(ctx) == nil {
				continue
			} else {
				return
			}
		} else {
			break
		}
	}
	return
}
