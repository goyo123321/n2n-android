package internal

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

type RouteAction string

const (
	ActionP2P    RouteAction = "p2p"
	ActionDirect RouteAction = "direct"
	ActionProxy  RouteAction = "proxy"
	ActionBlock  RouteAction = "block"
)

type RoutingRule struct {
	Name        string   `json:"name,omitempty"`
	IP          []string `json:"ip,omitempty"`
	Domain      []string `json:"domain,omitempty"`
	Port        string   `json:"port,omitempty"`
	OutboundTag string   `json:"outboundTag"`
}

type RoutingConfig struct {
	DomainStrategy string        `json:"domainStrategy,omitempty"`
	Rules          []RoutingRule `json:"rules"`
}

type compiledRule struct {
	name       string
	action     RouteAction
	cidrs      []*net.IPNet
	ports      []portRange
	domainSet  map[string]bool
	domainSufs []string
}

type portRange struct{ lo, hi int }

type Router struct {
	mu    sync.RWMutex
	rules []*compiledRule
}

func NewRouter(cfg *RoutingConfig) (*Router, error) {
	r := &Router{}
	if cfg == nil {
		return r, nil
	}
	for _, raw := range cfg.Rules {
		rule, err := compileRule(raw)
		if err != nil {
			return nil, fmt.Errorf("规则 %q 编译失败: %w", raw.Name, err)
		}
		r.rules = append(r.rules, rule)
	}
	return r, nil
}

func compileRule(raw RoutingRule) (*compiledRule, error) {
	c := &compiledRule{name: raw.Name, action: RouteAction(raw.OutboundTag), domainSet: make(map[string]bool)}
	for _, cidr := range raw.IP {
		_, n, err := net.ParseCIDR(cidr)
		if err != nil {
			ip := net.ParseIP(cidr)
			if ip == nil {
				return nil, fmt.Errorf("无效 CIDR/IP: %s", cidr)
			}
			if v4 := ip.To4(); v4 != nil {
				_, n, _ = net.ParseCIDR(cidr + "/32")
			} else {
				_, n, _ = net.ParseCIDR(cidr + "/128")
			}
		}
		if n != nil {
			c.cidrs = append(c.cidrs, n)
		}
	}
	if raw.Port != "" {
		for _, p := range strings.Split(raw.Port, ",") {
			p = strings.TrimSpace(p)
			if strings.Contains(p, "-") {
				parts := strings.SplitN(p, "-", 2)
				lo, _ := strconv.Atoi(parts[0])
				hi, _ := strconv.Atoi(parts[1])
				if lo > 0 && hi >= lo {
					c.ports = append(c.ports, portRange{lo, hi})
				}
			} else {
				v, _ := strconv.Atoi(p)
				if v > 0 {
					c.ports = append(c.ports, portRange{v, v})
				}
			}
		}
	}
	for _, d := range raw.Domain {
		if strings.HasPrefix(d, "domain:") {
			val := strings.TrimPrefix(d, "domain:")
			c.domainSet[val] = true
			c.domainSufs = append(c.domainSufs, "."+val)
		} else {
			c.domainSet[d] = true
			c.domainSufs = append(c.domainSufs, "."+d)
		}
	}
	return c, nil
}

func (r *Router) Match(dstIP string, dstPort int, domain string) (RouteAction, string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ip := net.ParseIP(dstIP)

	for _, rule := range r.rules {
		if rule.matches(ip, dstPort, domain) {
			return rule.action, rule.name
		}
	}
	if domain != "" && IsChinaDomain(domain) {
		return ActionDirect, "geosite:cn"
	}
	if ip != nil && IsChinaIP(dstIP) {
		return ActionDirect, "geoip:cn"
	}
	return ActionProxy, "default"
}

func (c *compiledRule) matches(ip net.IP, port int, domain string) bool {
	if len(c.cidrs) > 0 && ip != nil {
		hit := false
		for _, n := range c.cidrs {
			if n.Contains(ip) {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	if len(c.ports) > 0 {
		hit := false
		for _, pr := range c.ports {
			if port >= pr.lo && port <= pr.hi {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	if len(c.domainSet) > 0 || len(c.domainSufs) > 0 {
		if domain == "" {
			return false
		}
		d := strings.ToLower(domain)
		if c.domainSet[d] {
			return true
		}
		for _, suf := range c.domainSufs {
			if strings.HasSuffix(d, suf) || d == strings.TrimPrefix(suf, ".") {
				return true
			}
		}
		return false
	}
	return true
}

func ParseRoutingConfig(jsonStr string) (*RoutingConfig, error) {
	var cfg RoutingConfig
	if err := json.Unmarshal([]byte(jsonStr), &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// DefaultRoutingConfig 默认路由规则
//
// 顺序敏感：先匹配先命中
//   1. edge-p2p      → 10.64.0.0/24 走 n2n 组网
//   2. local-bypass  → 私网/环回走物理网络
//   3. default-proxy → 其他走 Workers 出口
//
// 未命中以上规则时，Match() 里还会按 IsChinaDomain / IsChinaIP 兜底走 direct。
//
// 关于 Telegram：不单独列直连规则。Telegram 直连通常需要本地网络能直连
// Telegram DC 才行，国内运营商到 Telegram 被阻断的情况下直连必然失败。
// 让 Telegram 走 default-proxy 经由 Workers 出口（或 TURN 中继）转发。
func DefaultRoutingConfig() *RoutingConfig {
	return &RoutingConfig{
		DomainStrategy: "IPIfNonMatch",
		Rules: []RoutingRule{
			{
				Name:        "edge-p2p",
				IP:          []string{"10.64.0.0/24"},
				OutboundTag: "p2p",
			},
			{
				Name: "local-bypass",
				IP: []string{
					"127.0.0.0/8",
					"10.0.0.0/8",
					"172.16.0.0/12",
					"192.168.0.0/16",
					"169.254.0.0/16",
				},
				OutboundTag: "direct",
			},
			{
				Name:        "default-proxy",
				OutboundTag: "proxy",
			},
		},
	}
}
