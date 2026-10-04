// Package mobile 是 gomobile 的桥接层。
package mobile

import (
	"log"
	"sync"
	"time"

	"github.com/goyo123321a/n2n-android/mobile/internal"
)

type Config struct {
	SignalingURL string
	RoomID       string
	ClientID     string
	NodeName     string
	ConnectToken string
	ShareDir     string
}

type Client struct {
	mu        sync.Mutex
	tunFd     int
	running   bool
	virtualIP string
	clientID  string
	edge      *internal.Edge
}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) SetTunFD(fd int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tunFd = fd
}

func (c *Client) Start(cfg *Config) string {
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
	c.mu.Unlock()

	icfg := &internal.Config{
		SignalingURL: cfg.SignalingURL,
		RoomID:       cfg.RoomID,
		ClientID:     cfg.ClientID,
		NodeName:     cfg.NodeName,
		ConnectToken: cfg.ConnectToken,
		ShareDir:     cfg.ShareDir,
	}

	edge, err := internal.Start(icfg, tunFd)
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

	log.Printf("[mobile] started, room=%s", cfg.RoomID)
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
