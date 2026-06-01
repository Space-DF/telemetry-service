package amqp

import (
	"fmt"
	"net/url"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type pooledConnection struct {
	conn     *amqp.Connection
	refCount int
}

type VhostConnectionPool struct {
	mu            sync.Mutex
	connections   map[string]*pooledConnection
	brokerURL     string
	allowedVHosts []string
}

func NewVhostConnectionPool(brokerURL string, allowedVHosts []string) *VhostConnectionPool {
	return &VhostConnectionPool{
		connections:   make(map[string]*pooledConnection),
		brokerURL:     brokerURL,
		allowedVHosts: allowedVHosts,
	}
}

func (p *VhostConnectionPool) ShouldHandle(vhost string) bool {
	if len(p.allowedVHosts) == 0 {
		return true
	}
	for _, allowed := range p.allowedVHosts {
		if allowed == vhost {
			return true
		}
	}
	return false
}

func (p *VhostConnectionPool) Get(vhost string) (*amqp.Connection, error) {
	p.mu.Lock()
	if pooled, exists := p.connections[vhost]; exists {
		if pooled.conn != nil && !pooled.conn.IsClosed() {
			pooled.refCount++
			p.mu.Unlock()
			return pooled.conn, nil
		}
		delete(p.connections, vhost)
	}
	p.mu.Unlock()

	vhostURL, err := p.buildURL(vhost)
	if err != nil {
		return nil, err
	}

	conn, err := amqp.Dial(vhostURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to vhost %s: %w", vhost, err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.connections[vhost] = &pooledConnection{
		conn:     conn,
		refCount: 1,
	}

	return conn, nil
}

func (p *VhostConnectionPool) Release(vhost string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pooled, exists := p.connections[vhost]
	if !exists {
		return
	}

	pooled.refCount--
	if pooled.refCount <= 0 {
		_ = pooled.conn.Close()
		delete(p.connections, vhost)
	}
}

func (p *VhostConnectionPool) InvalidateAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connections = make(map[string]*pooledConnection)
}

func (p *VhostConnectionPool) buildURL(vhost string) (string, error) {
	parsed, err := url.Parse(p.brokerURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse broker url: %w", err)
	}

	if vhost == "" {
		parsed.Path = "/"
		parsed.RawPath = ""
	} else {
		encoded := "/" + url.PathEscape(vhost)
		parsed.Path = encoded
		parsed.RawPath = encoded
	}

	return parsed.String(), nil
}
