// Package mobile 是 gomobile 的桥接层。
package mobile

import (
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"

	"github.com/goyo123321a/n2n-android/mobile/internal"
)

type Config struct {
	SignalingURL  string
	RoomID        string
	ClientID      string
	NodeName      string
	ConnectToken  string
	ShareDir      string
	PreferredIP   string
	PreferredPort int
}

// Protector 由 Kotlin 侧实现，调用 VpnService.protect(fd)
type Protector interface {
	Protect(fd int) bool
}

type Client struct {
	mu        sync.Mutex
	tunFd     int
	udpFd     int
	stunFd    int
	running   bool
	virtualIP string
	clientID  string
	edge      *internal.Edge
	protector Protector
}

func NewClient() *Client { return &Client{} }

func (c *Client) SetTunFD(fd int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tunFd = fd
}

func (c *Client) SetUdpFD(fd int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.udpFd = fd
}

func (c *Client) SetStunFD(fd int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stunFd = fd
}

// SetProtector 由 Kotlin 侧调用，传入 VpnService.protect 包装
func (c *Client) SetProtector(p Protector) {
	c.mu.Lock()
	c.protector = p
	c.mu.Unlock()

	internal.SetProtector(func(fd int) bool {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[protector] panic: %v", r)
			}
		}()
		return p.Protect(fd)
	})
}

func (c *Client) FetchVirtualIP(cfg *Config) (result string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[mobile] FetchVirtualIP panic: %v\n%s", r, debug.Stack())
			result = ""
		}
	}()

	icfg := &internal.Config{
		SignalingURL: cfg.SignalingURL, RoomID: cfg.RoomID,
		ClientID: cfg.ClientID, NodeName: cfg.NodeName,
		ConnectToken: cfg.ConnectToken, ShareDir: cfg.ShareDir,
		PreferredIP: cfg.PreferredIP, PreferredPort: cfg.PreferredPort,
	}
	return internal.FetchVirtualIP(icfg)
}

func (c *Client) Start(cfg *Config) (errMsg string) {
	defer func() {
		if r := recover(); r != nil {
			errMsg = fmt.Sprintf("panic: %v", r)
			log.Printf("[mobile] Start panic: %v\n%s", r, debug.Stack())
		}
	}()

	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return "already running"
	}
	if c.tunFd <= 0 {
		c.mu.Unlock()
		return "tun fd not set"
	}
	tunFd := c.tunFd
	udpFd := c.udpFd
	stunFd := c.stunFd
	c.mu.Unlock()

	icfg := &internal.Config{
		SignalingURL: cfg.SignalingURL, RoomID: cfg.RoomID,
		ClientID: cfg.ClientID, NodeName: cfg.NodeName,
		ConnectToken: cfg.ConnectToken, ShareDir: cfg.ShareDir,
		PreferredIP: cfg.PreferredIP, PreferredPort: cfg.PreferredPort,
	}

	edge, err := internal.Start(icfg, tunFd, udpFd, stunFd)
	if err != nil {
		return err.Error()
	}

	c.mu.Lock()
	c.edge = edge
	c.running = true
	if cfg.ClientID != "" {
		c.clientID = cfg.ClientID
	} else {
		c.clientID = edge.GetClientID()
	}
	c.mu.Unlock()

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			c.mu.Lock()
			running := c.running
			edgeRef := c.edge
			c.mu.Unlock()
			if !running || edgeRef == nil {
				return
			}
			if vip := edgeRef.GetVirtualIP(); vip != "" {
				c.mu.Lock()
				c.virtualIP = vip
				c.mu.Unlock()
			}
			select {
			case <-edgeRef.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	log.Printf("[mobile] started, room=%s, udpFd=%d, stunFd=%d", cfg.RoomID, udpFd, stunFd)
	return ""
}

func (c *Client) Stop() {
	c.mu.Lock()
	edge := c.edge
	c.edge = nil
	c.running = false
	c.virtualIP = ""
	c.mu.Unlock()
	if edge != nil {
		edge.Stop()
	}
	log.Printf("[mobile] stopped")
}

func (c *Client) GetStatus() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.running {
		return "stopped"
	}
	if c.virtualIP == "" {
		return "starting"
	}
	return "running"
}

func (c *Client) GetVirtualIP() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.virtualIP
}

func (c *Client) GetClientID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clientID
}

func (c *Client) GetPeersJSON() string {
	c.mu.Lock()
	edge := c.edge
	c.mu.Unlock()
	if edge == nil {
		return "[]"
	}
	return edge.GetPeersJSON()
}

// ============ 日志（package-level 静态方法）============

func GetLogs() string {
	return internal.GetLogs()
}

func ClearLogs() {
	internal.ClearLogs()
}
