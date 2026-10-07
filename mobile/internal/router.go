// DefaultRoutingConfig 默认路由规则
//
// 顺序敏感：先匹配先命中。
//
// 关键设计：**不要**加 default-proxy 规则！
// 因为 Match() 里已经有兜底：
//   1. IsChinaDomain(domain)  → direct
//   2. IsChinaIP(ip)          → direct
//   3. 其他                   → proxy
//
// 如果加了空条件的 default-proxy 规则，会抢在 geoip 判定之前命中，
// 导致所有流量都走 proxy。
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
			// ★ 这里故意不加 default-proxy
			//   由 Match() 的兜底逻辑处理（geoip / geosite / 默认 proxy）
		},
	}
}
