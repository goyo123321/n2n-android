package internal

import (
	_ "embed"
	"log"
	"regexp"
	"strings"
	"sync"
)

//go:embed data/geosite_cn.txt
var geositeCNData string

var (
	geositeCNMu      sync.RWMutex
	geositeCNFull    map[string]bool
	geositeCNDomain  []string
	geositeCNKeyword []string
	geositeCNRegex   []*regexp.Regexp
)

func init() {
	geositeCNFull = make(map[string]bool)
	lines := strings.Split(geositeCNData, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// ★ 跳过非域名类指令
		if strings.HasPrefix(line, "include:") ||
			strings.HasPrefix(line, "ext:") ||
			strings.HasPrefix(line, "attribute:") {
			continue
		}

		line = strings.ToLower(line)

		// ★ 剥离 @ 属性后缀
		if atIdx := strings.Index(line, " @"); atIdx >= 0 {
			line = strings.TrimSpace(line[:atIdx])
		}

		switch {
		case strings.HasPrefix(line, "full:"):
			geositeCNFull[strings.TrimPrefix(line, "full:")] = true
		case strings.HasPrefix(line, "domain:"):
			geositeCNDomain = append(geositeCNDomain, strings.TrimPrefix(line, "domain:"))
		case strings.HasPrefix(line, "keyword:"):
			geositeCNKeyword = append(geositeCNKeyword, strings.TrimPrefix(line, "keyword:"))
		case strings.HasPrefix(line, "regexp:"):
			pat := strings.TrimPrefix(line, "regexp:")
			if re, err := regexp.Compile(pat); err == nil {
				geositeCNRegex = append(geositeCNRegex, re)
			}
		default:
			geositeCNDomain = append(geositeCNDomain, line)
		}
	}
	log.Printf("[GeoSite] full=%d domain=%d keyword=%d regexp=%d",
		len(geositeCNFull), len(geositeCNDomain), len(geositeCNKeyword), len(geositeCNRegex))
}

func IsChinaDomain(domain string) bool {
	if domain == "" {
		return false
	}
	d := strings.ToLower(strings.TrimSuffix(domain, "."))
	geositeCNMu.RLock()
	defer geositeCNMu.RUnlock()

	if geositeCNFull[d] {
		return true
	}
	for _, suf := range geositeCNDomain {
		if d == suf || strings.HasSuffix(d, "."+suf) {
			return true
		}
	}
	for _, kw := range geositeCNKeyword {
		if strings.Contains(d, kw) {
			return true
		}
	}
	for _, re := range geositeCNRegex {
		if re.MatchString(d) {
			return true
		}
	}
	return false
}
